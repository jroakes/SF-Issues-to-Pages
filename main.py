import os
import shutil

from lib.process import *

from lib.crawl import crawler
from lib.sheets import load_all_issues_to_sheets, load_ouput_data_to_sheet



def run_crawl(url, data=None):
    default_data = {
    "data_path": "data"
    }

    if isinstance(data, dict):
        default_data.update(data)

    data = default_data

    if not os.path.exists(data['data_path']):
        print('Creating data folder...')
        os.makedirs(data['data_path'])
    
    if not isinstance(url, str):
        print('Please enter a valid URL.')
        return False
    
    res = crawler(url=url, output_folder=data['data_path'])
    print('Crawl Complete!')
    return res


def run_reports(url, data=None):

    default_data = {
    "data_path": "data",
    "images_file": "all_image_inlinks.csv",
    "links_file": "all_inlinks.csv",
    "urls_file": "internal_html.csv",
    "output_name": "Issue Report: Sitewide and Authoring",
    "percent_threshold": 0.3,
    "run_version": "2"
    }

    if isinstance(data, dict):
        default_data.update(data)

    data = default_data

    all_issues_report = read_issues_report(filename=f"{data['data_path']}/issues_reports/issues_overview_report.csv")
    issues_report_authoring = read_authoring_issues_report(all_issues_report)
    issues_report_sitewide = read_sitewide_issues_report(all_issues_report)

    issues = read_issue_data(path=f"{data['data_path']}/issues_reports")

    # Need this file: https://app.screencast.com/JUxEBeStBJ1GW
    images_authoring = read_authoring_image_data(filename=f"{data['data_path']}/{data['images_file']}")

    # Need this file: https://app.screencast.com/zITnr6P9t1nGL
    links_authoring = read_authoring_link_data(filename=f"{data['data_path']}/{data['links_file']}")

    # Need this file: https://app.screencast.com/qREphIzkPzEkT
    url_df = read_valid_urls(filename=f"{data['data_path']}/{data['urls_file']}")

    # Build Authoring Data
    authoring_df = map_issues_to_urls_authoring(url_df, issues_report_authoring, issues)
    authoring_df = parse_url_images(authoring_df, images_authoring)
    authoring_df = parse_url_links(authoring_df, links_authoring)


    # Build Sitewide Data
    issues_report_sitewide = map_url_counts_to_issues_sitewide(url_df, issues_report_sitewide, issues)
    link_issues_sitewide = read_sitewide_link_data(url_df, filename=f"{data['data_path']}/{data['links_file']}", percent_threshold=data['percent_threshold'])
    image_issues_sitewide = read_sitewide_image_data(url_df, filename=f"{data['data_path']}/{data['images_file']}", percent_threshold=data['percent_threshold']) 


    # Load all issues to Google Sheets
    issue_sheet_mapping = load_all_issues_to_sheets(issues, url, version=data['run_version'])

    # Add sheets URLS to issues_report_sitewide
    issues_report_sitewide['Examples'] = issues_report_sitewide['Issue Data Name'].map(lambda x: f'=HYPERLINK("{issue_sheet_mapping[x]}", "Examples")')
    issues_report_sitewide.drop(columns=['Issue Data Name'], inplace=True)


    output_data = {
        'All Issues': all_issues_report,
        'Authoring Issues': issues_report_authoring,
        'Sitewide Issues': issues_report_sitewide,
        'Authoring URL Issues': authoring_df,
        'Sitewide Link Issues': link_issues_sitewide,
        'Sitewide Image Issues': image_issues_sitewide
        }


    sheet_url = load_ouput_data_to_sheet(data["output_name"], output_data, url, version=data['run_version'])

    print(f'Issue Report: {sheet_url}')
    print('Complete!')



    return True




def run_audit(url, data=None):


    # Getting the list of directories
    dir = os.listdir("data")
    
    # Checking if the list is empty or not
    if not len(dir) == 0:
        print("Data directory is not empty. Please delete the contents of the data directory before running the audit.")
        res = input("Would you like to delete the contents of the data directory? (y/n): ")
        if res == "y":
            print("Deleting contents of data directory...")
            shutil.rmtree("data")
            os.makedirs("data")
        else:
            print("Please delete the contents of the data directory before running the audit.")
            return False

    if not run_crawl(url, data):
        return False

    if not run_reports(url, data):
        return False

    return True
