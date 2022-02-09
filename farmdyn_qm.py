#!/usr/bin/env python
# Import libraries
import email.message
import glob
import json
import os
import requests
import smtplib
import subprocess
from distutils.dir_util import copy_tree
import gdxpds
import secret


def run_revision_batch(rev_num, batch_name, batch_file, fd_dir):
    os.chdir(fd_dir)
    print("Reverting directory")
    revert()
    print("Done reverting")

    # --- (1) update the working copy to the specified revision
    if isinstance(rev_num, int):
        # update to revision
        print("Updating SVN")
        update_to_revision(rev_num)
        # get current revision number
        cur_rev = get_cur_revision()
        print(f"Updated to rev {cur_rev}")
    elif rev_num != 'WorkingCopy':
        return f'Invalid revision number {rev_num} entered as a function argument'

    # --- (2) create a folder for the results in the QM
    #         if it already exists, check if it contains files -> in that case don't execute batch again
    rev_num_dir = os.path.join('..\QM', str(rev_num), batch_name)
    print("Creating output dir")
    if os.path.isdir(rev_num_dir):
        files_rev_dir = os.listdir(rev_num_dir)
        if len(files_rev_dir) > 0:
            return 'Done'
    else:
        os.makedirs(rev_num_dir, exist_ok=True)
    print("Deleting old files")
    # --- (3) clean up results from previous runs
    delete_files_glob('results/expFarms/*')

    # --- (4) first, change the output directory in the batch file to a directory where we know we
    #         have read/write access, and it's consistent
    #         Then, clean up (or first create) the output directory, so we don't copy old results later
    #         Afterwards, run the batch file and send results to CouchDB

    # Navigate into the GUI folder
    os.chdir('gui/')
    print("Replacing output dir in batch")
    output_dir = replace_output_dir_batch(batch_file)

    # --- clean up previous results from output dir if it exists
    print("Deleting old files")
    if os.path.isdir(output_dir):
        delete_files_glob(output_dir + '/*')
    else:
        os.mkdir(output_dir)

    # --- run batch file in GAMS
    run_batch_gams(batch_file)
    # --- send results to CouchDB
    send_to_db(cur_rev, batch_name, fd_dir)

    # --- (5) copy results to according QM Revision folder
    # Go back to main FarmDyn folder
    os.chdir(fd_dir)

    results_dir = os.path.join('results', 'expFarms')
    copy_tree(results_dir, rev_num_dir)

    # --- also copy all results from output dir to results dir
    if os.path.isdir(output_dir):
        list_outputs = [f.path for f in os.scandir(output_dir) if f.is_dir()]
        for directory in list_outputs:
            copy_tree(directory, rev_num_dir)

    return cur_rev


def cleanup():
    print(subprocess.run(['r/qm/svn/svn.exe', 'cleanup'], capture_output=True, text=True).stderr.strip())


def revert():
    print(subprocess.run(['r/qm/svn/svn.exe', 'revert', '-R', './'], capture_output=True, text=True).stderr.strip())


def update_to_head():
    print(subprocess.run(['r/qm/svn/svn.exe', 'up', '-r', 'HEAD', '--accept=theirs-full'], capture_output=True,
                         text=True).stderr.strip())


def update_to_revision(rev_num):
    print(subprocess.run(['r/qm/svn/svn.exe', 'up', '-r', str(rev_num)], capture_output=True, text=True).stderr.strip())


def get_cur_revision():
    call = subprocess.run(['r/qm/svn/svn.exe', 'info', '--show-item', 'revision'], capture_output=True, text=True)
    return int(call.stdout.strip())


def delete_files_glob(glob_string):
    file_list = glob.glob(glob_string)
    # Iterate over the list of filepaths & remove each file
    for file_path in file_list:
        try:
            os.remove(file_path)
        except Exception as e:
            print(e)
            print(f'Error while deleting file : {file_path}')


def replace_output_dir_batch(file_path):
    # read the batch file into memory
    with open(file_path, 'r+') as file:
        batch_file = file.readlines()
        # get the home folder of the current user, bc we have read/write access there
        user_profile = os.getenv('userprofile')
        # add qmTemp to the home folder path (will be the directory where all results of the QM will be temp. stored)
        qm_temp_dir = os.path.join(user_profile, 'qmTemp')

        # loop through lines of the batch file until the "output dir" line is found,
        # then replace with our new output file path.
        # remove gurobi as a solver option because we currently don't have a license -> use CPLEX instead
        for i in range(len(batch_file)):
            if 'output dir =' in batch_file[i]:
                batch_file[i] = f'output dir = {qm_temp_dir}\n'
            elif 'Solver = GUROBI' in batch_file[i]:
                batch_file[i] = 'Solver = CPLEX'

        # write the changes to the batch file and return our new output dir to the 
        # calling process
        file.truncate(0)
        file.writelines(batch_file)
        file.close()
        return qm_temp_dir


def run_batch_gams(batch_file):
    subprocess.run(['java', '-Xmx1G', '-Xverify:none', '-XX:+UseParallelGC', '-XX:PermSize=20M', '-XX:MaxNewSize=32M',
                    '-XX:NewSize=32M', '-Djava.library.path=jars', '-classpath', 'jars/gig.jar',
                    'de.capri.ggig.BatchExecution', 'dairydyn.ini', 'dairydyn_default.xml', batch_file])


def dict_creation(rev_num, batch_name, fd_dir):
    # --- (1) set working directory to root dir
    os.chdir(fd_dir)
    os.chdir('results/expFarms/')

    # --- (2) get a list of all .gdx files that begin with res_ 
    gdx_files = glob.glob('res_*.gdx')

    # --- (3) Loop through gdx files resulting from batch execution
    gdx_list = []
    for gdx_file in gdx_files:
        print(f"Processing file: {gdx_file}")
        # convert gdx file to DataFrame
        df = gdxpds.to_dataframe(gdx_file, 'p_sumRes')['p_sumRes']
        # remove first column (Base)
        df = df.iloc[:, 1:]
        # set index
        df = df.set_index('*')

        # format DataFrame to Json (dict)
        data_dict = df.to_dict()
        data_dict = data_dict['Value']

        scenario = os.path.splitext(gdx_file)[0][4:]
        id = f'{rev_num}::{batch_name}::{scenario}'

        res_dict = {'_id': id, 'revision': rev_num, 'batch_name': batch_name, 'scenario': scenario}
        res_dict.update(data_dict)

        gdx_list.append(res_dict)
    return gdx_list


def send_to_db(rev_num, batch_name, fd_dir):
    url = 'https://fruchtfolge.agp.uni-bonn.de/db/farmdyn_qm/'
    gdx_list = dict_creation(rev_num, batch_name, fd_dir)
    for data in gdx_list:
        rest = requests.post(url, data=json.dumps(data), headers={'content-type': 'application/json'})
        print(rest)


def send_mail(rev_num):
    last_author = subprocess.run(['r/qm/svn/svn.exe', 'info', '--show-item', 'last-changed-author'],
                                 capture_output=True, text=True).stdout.strip()

    authors = ['wolfgangb', 'juliah', 'davids', 'tillk', 'lennartk', 'christophp']
    names = ['Wolfgang', 'Julia', 'David', 'Till', 'Lennart', 'Christoph']
    emails = ['wolfgang.britz@ilr.uni-bonn.de', 'julia.heinrichs@ilr.uni-bonn.de', 'david.schaefer@ilr.uni-bonn.de',
              'till.kuhn@ilr.uni-bonn.de', 'lennart.kokemohr@ilr.uni-bonn.de', 'christoph.pahmeyer@ilr.uni-bonn.de']

    try:
        author_index = authors.index(last_author)
    except ValueError:
        author_index = authors.index('tillk')

    sender = secret.get_user()
    recipient = emails[author_index]
    # recipient = 'jannik.mielke@ilr.uni-bonn.de'
    # recipient = 'christoph.pahmeyer@ilr.uni-bonn.de'

    msg = email.message.Message()
    msg['Subject'] = f'FarmDyn QM results for revision {rev_num} are ready!'
    msg['From'] = sender
    msg['To'] = recipient
    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(f'''Hi {names[author_index]}!<br>
                    Thanks for contributing to FarmDyn :)<br>
                    You can find the results of the batch compilation and execution tests at the following website:<br>
                    <a href="https://chrispahm.github.io/farmdyn-qm-client/">https://chrispahm.github.io/farmdyn-qm-client/</a>
                    Furthermore, the listing and include files are store in the following folder:<br>
                    <a href='N:\\em\\work1\\FarmDyn\\FarmDyn_QM\\QM\\{rev_num}'>N:\\em\\work1\\FarmDyn\\FarmDyn_QM\\QM\\{rev_num}</a>
                    ''')

    mail = smtplib.SMTP('mail.uni-bonn.de', 587)
    mail.ehlo()
    mail.starttls()
    mail.login(sender, secret.get_pwd())
    mail.sendmail(sender, recipient, msg.as_string())
    mail.quit()


def main():
    fd_dir = r'C:\FarmDyn\FarmDyn_QM'

    # set working directory to FarmDyn GUI folder
    os.chdir(fd_dir + '/gui')
    # get list of batch files to run
    batch_files = glob.glob('batch_test_*.txt')
    # remove compilation test from list
    batch_files.remove('batch_test_compilation.txt')
    # set working directory back to main folder
    os.chdir(fd_dir)
    # get names
    batch_names = list(map(lambda x: os.path.splitext(x)[0], batch_files))

    cleanup()
    update_to_head()
    latest_rev = get_cur_revision()
    print(f"Found latest revision: {latest_rev}")

    for batch_name, batch_file in zip(batch_names, batch_files):
        print(run_revision_batch(latest_rev, batch_name, batch_file, fd_dir))

    send_mail(latest_rev)


if __name__ == '__main__':
    main()
