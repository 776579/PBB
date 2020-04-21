import csv
import os
import sys
import smtplib

from email.message import EmailMessage
from pprint import pprint

import argparse


REQUEST_FIELDNAMES = ['Terms',
                      'Name',
                      'Email',
                      'Date',
                      'Key']


def construct_terms(terms):
    if not isinstance(terms, list):
        try:
            terms = terms.split(',')
            terms = [term.lower().strip() for term in terms]
        except Exception:
            sys.exit(f'\n❗️ Unsupported format of search terms:'
                     f'{terms} {type(terms)}')
    return terms


def search(terms):
    if not isinstance(terms, list):
        sys.exit('❗️Search terms are not in a list.')
    else:
        results = {}
        for dataset in datasets:
            with open(f'{args.folder_path}/{dataset}') as fo:
                if args.verbose:
                    print(f'\n➤ Searching {dataset}')
                reader = csv.DictReader(fo)
                for row in reader:
                    try:
                        gid = row['GlobalID']
                    except ValueError as e:
                        sys.exit(f'❗️Unable to get GID in {dataset}')
                    for fieldname, value in row.items():
                        # Set target string
                        target = ''
                        if fieldname and args.include_fieldnames:
                            # Debugging
                            # if not isinstance(fieldname, str):
                            #     print(f'⚠️ Casting fieldname '\
                            #           f'{fieldname}, {type(fieldname)}.')
                            # ---
                            target += str(fieldname) + ' '
                        if fieldname and value:
                            # Debugging
                            # if not isinstance(value, str):
                            #     print(f'⚠️ Casting value '\
                            #           f'{value}, {type(value)}.')
                            # ---
                            target += str(value)
                        # Search and compose results dict
                        if all(term in target.lower() for term in terms):
                            if dataset not in results.keys():
                                results[dataset] = {fieldname: [gid]}
                            elif fieldname not in results[dataset].keys():
                                results[dataset][fieldname] = [gid]
                            else:
                                results[dataset][fieldname].append(gid)
            if args.verbose:
                if dataset in results.keys():
                    for fieldname in results[dataset]:
                        print(f'-->{len(results[dataset][fieldname])} '
                              f'hit(s) for {fieldname}')
                        if args.terms:
                            print(f'[{",".join(results[dataset][fieldname])}]')
                print(f'✓ Done.')

        return results


def dataset_url(dataset_name, gids):
    if isinstance(gids, list):
        dataset_url = (f'https://lk.eicc.emory.edu:8172/'
                       f'PBB%20Case%20Scenario/query-executeQuery.view'
                       f'?schemaName=study&query.queryName={str(dataset_name)}'
                       f'&query.GlobalID~in={";".join(gids)}')
        return dataset_url
    else:
        sys.exit(f'\n❗️Failed to generate dataset URL due to non-list GIDs.')


def email_requestor(request, results):
    print(f'\n📬 Emailing {request["Name"]} {request["Email"]} '
          f'for request {request["Key"]}...')
    # Prep CSV attachment
    # Temporary files are named with request filename appended by request id
    results_file = args.request_file.replace(
        '.csv', f'_{request["Key"]}.csv')

    with open(results_file, 'w') as fp:
        writer = csv.DictWriter(
            fp, fieldnames=['Dataset', 'Fieldname', 'Matches', 'GIDs', 'URL'])
        writer.writeheader()
        for dataset in results.keys():
            for fieldname in results[dataset].keys():
                gids = results[dataset][fieldname]
                dataset_name = dataset.replace('.csv', '')
                writer.writerow({
                    'Dataset': dataset_name,
                    'Fieldname': fieldname,
                    'Matches': len(gids),
                    'GIDs': ';'.join(gids),
                    'URL': dataset_url(dataset_name, gids),
                })
        print(f'📎 Results file saved as {os.path.abspath(results_file)}.')
    # Set email headers
    msg = EmailMessage()
    msg['Subject'] = f'Results for query {request["Key"]}'
    msg['From'] = 'pbb@eicc.emory.edu'
    msg['To'] = request['Email']
    # Set Email body
    msg.set_content(
        f'{request["Name"]},'
         '\n\nPlease see the attached results of your search request.'
         '\n\nDD NOT reply to this email.')
    # Attach csv to Email
    with open(results_file, 'r') as fp:
        csv_data = fp.read()
        # Only use the last part of absolute file name
        attachment_filename = results_file.split('/')[-1]
        msg.add_attachment(csv_data, filename=attachment_filename)
    # Connect to SMTP server
    try:
        smtp = smtplib.SMTP('smtp.service.emory.edu')
    except Exception as err:
        sys.exit(f'❗️Failed to initialize connection to SMTP server. '
                 f'Detail: {err}')
    # Send email
    try:
        non_deliveries = smtp.send_message(msg)
    except Exception as err:
        sys.exit(f'❗️Failed to send email. Detail: {err}')
    else:
        if bool(non_deliveries):
            # If message failed to deliver
            print(f'\n⚠️ Email failed to deliver. Detail: '
                  f'{pprint(non_deliveries)} ')
        else:
            print('✉️ Sent.')



def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-f', '--folder_path',
                        required=True,
                        type=str,
                        help='Relative path to data dump folder with CSV.')
    group.add_argument('-t', '--terms',
                       type=str,
                       help='Specify comma seperated search term(s).')
    group.add_argument('-r', '--request_file',
                       type=str,
                       help='Specify request file.')
    parser.add_argument('-v', '--verbose',
                        action="store_true",
                        help='Print progress and results in console.')
    parser.add_argument('--include_fieldnames',
                        action="store_true",
                        help='Search fieldnames as well.')
    # TODO:
    # parser.add_argument('-i', '--interactive', action="store_true", help='Interactive mode.')
    # parser.add_argument('-s', '--save_results', action="store_true", help='Save results to csv file.')

    global args
    args = parser.parse_args()

    # Validate csv folder path
    global datasets
    datasets = []

    if os.path.isdir(args.folder_path):
        filenames = os.listdir(args.folder_path)
        for filename in filenames:
            if filename.lower().endswith('.csv'):
                datasets.append(filename)
        if not datasets:
            sys.exit('⚠️ No CSV found in data dump folder.')
        if args.verbose:
            print(f'\n⚙ {len(datasets)} datasets found in data dump folder: ')
            pprint(datasets)
    else:
        sys.exit('❗️ Invalid data dump folder path.')

    if args.request_file:
        # Validate query requests
        if os.path.isfile(args.request_file):
            with open(args.request_file) as fo:
                reader = csv.DictReader(fo)
                if reader.fieldnames != REQUEST_FIELDNAMES:
                    sys.exit(
                        f'❗️ Failed to parse request {args.request_file}.'
                        f'Expected field names are {REQUEST_FIELDNAMES}.')
                else:
                    for row in reader:
                        request = {'Terms': row['Terms'],
                                   'Name': row['Name'],
                                   'Email': row['Email'],
                                   'Date': row['Date'],
                                   'Key': row['Key']}
                        terms = construct_terms(request['Terms'])
                        if args.verbose:
                            print(f'\n👉 Processing request '
                                  f'{request["Key"]}: '
                                  f'\n\tRequestor: {request["Name"]} '
                                  f'{request["Email"]}'
                                  f'\n\tSearch terms: {terms}'
                                  f'\n\tSubmitted at {request["Date"]}')

                        results = search(terms)
                        # TODO: Validate email address
                        email_requestor(request, results)

                        if args.verbose:
                            print(f'\n✅ Completed request {request["Key"]}.')
        else:
            sys.exit('❗️ Invalid request file path.')
    elif args.terms:
        # Process entered terms
        terms = construct_terms(args.terms)
        if args.verbose:
            print(f'\n⚙ Search terms: {terms}')
            results = search(terms)
    else:
        sys.exit('\n⚠️ Empty search terms. Nothing to do.')


if __name__ == '__main__':
    main()
