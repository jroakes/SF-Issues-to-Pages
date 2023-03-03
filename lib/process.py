import os
import re
import pathlib
import pandas as pd
from tqdm.auto import tqdm
from urllib.parse import urlparse

import warnings
warnings.filterwarnings("ignore")

VALID_COLUMNS=["Address", "Status Code", "Indexability"]
AUTHORING_ISSUES = ['Response Codes: Internal Client Error (4xx)',
 'Security: HTTP URLs',
 'Canonicals: Canonicalised',
 'Hreflang: Non-200 hreflang URLs',
 'Response Codes: Internal Blocked Resource',
 'Response Codes: Internal No Response',
 'Canonicals: Non-Indexable Canonical',
 'Response Codes: External Blocked Resource',
 'PageSpeed: Image Elements Do Not Have Explicit Width & Height',
 'H1: Multiple',
 'Images: Over 100 KB',
 'PageSpeed: Defer Offscreen Images',
 'Page Titles: Over 60 Characters',
 'PageSpeed: Efficiently Encode Images',
 'H1: Missing',
 'Canonicals: Missing',
 'Page Titles: Duplicate',
 'Page Titles: Below 30 Characters',
 'PageSpeed: Properly Size Images',
 'Content: Low Content Pages',
 'Page Titles: Below 200 Pixels',
 'Images: Missing Alt Text',
 'Response Codes: External Server Error (5xx)',
 'Security: Unsafe Cross-Origin Links',
 'Response Codes: Internal Redirection (3xx)',
 'Links: Internal Outlinks With No Anchor Text',
 'H1: Duplicate',
 'Content: Readability Difficult',
 'URL: Over 115 Characters',
 'URL: Uppercase',
 'Meta Description: Over 155 Characters',
 'H2: Over 70 Characters',
 'Meta Description: Missing',
 'Links: Non-Descriptive Anchor Text In Internal Outlinks',
 'H2: Missing',
 'URL: Underscores',
 'URL: Parameters',
 'Meta Description: Duplicate',
 'Response Codes: External No Response',
 'Images: Missing Alt Attribute',
 'H1: Over 70 Characters',
 'Meta Description: Below 70 Characters',
 'Content: Readability Very Difficult',
 'Response Codes: External Client Error (4xx)',
 'PageSpeed: Use Video Formats for Animated Content',
 'Images: Alt Text Over 100 Characters',
 'URL: Multiple Slashes',
 'URL: Repetitive Path',
 'Page Titles: Same as H1',
 'Hreflang: Missing X-Default']


STATUS_30X = list({num for num in range(300, 400)})
STATUS_20X = list({num for num in range(200, 300)})
STATUS_40X = list({num for num in range(400, 500)})
STATUS_50X = list({num for num in range(500, 600)})

NON_ISSUE_FILES = ['issues_overview_report', 'all_image_inlinks', 'all_inlinks', 'internal_html']


def read_issue_data(path="data/issues_reports"):
    """Reads all the issues from the Issues folder and returns a dictionary of dataframes with the file name as the key"""
    issues = {}
    path_obj = pathlib.Path(path)
    num_files = len(list(path_obj.glob(f'*.csv')))

    for file in tqdm(path_obj.glob(f'*.csv'), desc='Reading Issues', total=num_files):
        if file.stem in NON_ISSUE_FILES:
            continue
        issues[file.stem] = pd.read_csv(file)
    return issues

def read_issues_report(filename="data/issues_reports/issues_overview_report.csv"):
    """Reads the issues overview report and returns a dataframe"""
    issues_report = pd.read_csv(filename)
    issues_report.drop(columns=["URLs", "% of Total"], inplace=True)
    # Map the issue name to a valid data column name
    issues_report['Issue Data Name'] = issues_report['Issue Name'].map(lambda x: re.sub(r'[\.\ ]+', '_', re.sub(r'[\:\-\&\<\>]+', '', x.lower())))
    return issues_report

def read_authoring_issues_report(issues_report):
    """Reads the issues overview report and returns a dataframe"""
    issues_report = issues_report[issues_report['Issue Name'].isin(AUTHORING_ISSUES)]
    return issues_report

def read_sitewide_issues_report(issues_report):
    """Reads the issues overview report and returns a dataframe"""
    issues_report = issues_report[~issues_report['Issue Name'].isin(AUTHORING_ISSUES)]
    return issues_report

def read_authoring_image_data(filename="data/all_image_inlinks.csv"):
    images = pd.read_csv(filename)
    images['Alt Text'] = images['Alt Text'].fillna("")
    images['Alt Text Length'] = images['Alt Text'].map(lambda x: len(x))
    images = images[(images['Size (Bytes)'] > 1000) & (images['Link Position'] == 'Content')]
    return images

def read_sitewide_image_data(df, filename="data/all_image_inlinks.csv", percent_threshold=0.8):
    """Reads the sitewide image data and returns a dataframe"""

    num_urls = len(df)
    images = pd.read_csv(filename)
    images['Alt Text'] = images['Alt Text'].fillna("")
    images['Alt Text Length'] = images['Alt Text'].map(lambda x: len(x))
    # Status cod matches 40X
    images['Status Code'] = images['Status Code'].fillna(0).astype(int)

    images_30X = images[images['Status Code'].isin(STATUS_30X)].copy()
    images_40X = images[images['Status Code'].isin(STATUS_40X)].copy()
    images_50X = images[images['Status Code'].isin(STATUS_50X)].copy()

    # List of Destinations of images with 30X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    images_30X = images_30X.groupby('Destination')['Source'].nunique().reset_index()
    images_30X = images_30X[images_30X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # List of Destinations of images with 40X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    images_40X = images_40X.groupby('Destination')['Source'].nunique().reset_index()
    images_40X = images_40X[images_40X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # List of Destinations of images with 50X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    images_50X = images_50X.groupby('Destination')['Source'].nunique().reset_index()
    images_50X = images_50X[images_50X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # Build Dataframe

    #Make lists the same length
    max_len = max(len(images_30X), len(images_40X), len(images_50X))
    images_30X = images_30X + [""] * (max_len - len(images_30X))
    images_40X = images_40X + [""] * (max_len - len(images_40X))
    images_50X = images_50X + [""] * (max_len - len(images_50X))

    data = {'30X Images': images_30X, '40X Images': images_40X, '50X Images': images_50X}

    return pd.DataFrame(data)


def read_sitewide_link_data(df, filename="data/all_inlinks.csv", percent_threshold=0.8):
    """Reads the sitewide link data and returns a dataframe"""

    num_urls = len(df)
    links = pd.read_csv(filename)
    links['Status Code'] = links['Status Code'].fillna(0).astype(int)
    links = links[(links['Type'] == "Hyperlink")]

    links_30X = links[links['Status Code'].isin(STATUS_30X)].copy()
    links_40X = links[links['Status Code'].isin(STATUS_40X)].copy()
    links_50X = links[links['Status Code'].isin(STATUS_50X)].copy()

    # List of Destinations of links with 30X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    links_30X = links_30X.groupby('Destination')['Source'].nunique().reset_index()
    links_30X = links_30X[links_30X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # List of Destinations of links with 40X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    links_40X = links_40X.groupby('Destination')['Source'].nunique().reset_index()
    links_40X = links_40X[links_40X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # List of Destinations of links with 50X status code that have distinct Source URL counts that is greater than or equal to percent_threshold of num_urls
    links_50X = links_50X.groupby('Destination')['Source'].nunique().reset_index()
    links_50X = links_50X[links_50X['Source'] >= num_urls * percent_threshold]['Destination'].tolist()

    # Build Dataframe

    #Make lists the same length
    max_len = max(len(links_30X), len(links_40X), len(links_50X))
    links_30X = links_30X + [""] * (max_len - len(links_30X))
    links_40X = links_40X + [""] * (max_len - len(links_40X))
    links_50X = links_50X + [""] * (max_len - len(links_50X))

    data = {'30X Links': links_30X, '40X Links': links_40X, '50X Links': links_50X}
    return pd.DataFrame(data)

def read_authoring_link_data(filename="data/all_inlinks.csv"):
    links = pd.read_csv(filename)
    links['Status Code'] = links['Status Code'].fillna(0).astype(int)
    links = links[(links['Type'] == "Hyperlink") & (links['Link Position'] == 'Content')]
    return links

def read_valid_urls(filename="data/internal_html.csv"):
    """Reads Screaming Frog file to dataframe and returns only VALID_COLUMNS"""
    df = pd.read_csv(filename).fillna(0)
    return df[[c for c in df.columns if c in VALID_COLUMNS]].fillna(0)

def parse_issues(url, issues, issues_report):
    """Returns a list of issues for a given url"""
    search_columns = ["Address", "Source"]
    issues_list = []
    for index, row in issues_report.iterrows():
        if row['Issue Data Name'] in issues:
            issue_data = issues[row['Issue Data Name']]
            issue_data_values = issue_data[[c for c in search_columns if c in issue_data.columns]].values
            issue_data_values = list(set([item for sublist in issue_data_values for item in sublist]))
            if url in issue_data_values:
                issues_list.append(row['Issue Name'])
    
    return issues_list

def map_issues_to_urls_authoring(df, issues_report, issues):
    """Returns issue if it exists in issue dataframes"""
    search_columns = ["Address", "Source"]
    for index, row in tqdm(df.iterrows(), desc="Parsing Issues", total=len(df)):
        url_issues = parse_issues(row['Address'], issues, issues_report)
        if len(url_issues) > 0:
            df.loc[index, 'Issues'] = "\n".join(url_issues)
    return df 

def map_url_counts_to_issues_sitewide(df, issues_report, issues):
    """Returns issue_report with url counts and percentages that match the issue"""
    search_columns = ["Address", "Source"]
    for index, row in tqdm(issues_report.iterrows(), desc="Mapping URL Counts", total=len(issues_report)):
        issue_data = issues[row['Issue Data Name']]
        # TODO: Check if this is the best way to do this
        issue_data_values = issue_data[[c for c in search_columns if c in issue_data.columns]].values
        issue_data_values = list(set([item for sublist in issue_data_values for item in sublist]))
        valid_urls = list(set(df['Address'].values))

        # Calculate URL counts and percentages
        url_count = len(set(issue_data_values).intersection(valid_urls))
        url_percent = url_count / len(valid_urls) * 100

        issues_report.loc[index, 'URL Count'] = url_count
        issues_report.loc[index, 'URL Percent'] = url_percent
    
    return issues_report

def parse_url_images(df, images):
    """Returns a list of image issues for a given url"""
    for index, row in tqdm(df.iterrows(), desc="Parsing Images", total=len(df)):
        url_images = images[images['Source'] == row['Address']]

        missing = list(set(url_images[url_images['Alt Text Length'] == 0]['Destination'].values))
        broken = list(set(url_images[~url_images['Status Code'].isin(STATUS_20X+STATUS_30X)]['Destination'].values))

        df.loc[index, 'Issue: Missing Alt Text'] = "\n".join(missing)
        df.loc[index, 'Issue: Broken Images'] = "\n".join(broken)
    return df 

def parse_url_links(df, links):
    """Returns a list of link issues for a given url"""
    for index, row in tqdm(df.iterrows(), desc="Parsing Links", total=len(df)):
        addr = row['Address']
        url_links = links[links['Source'] == addr]
        domain = f"{urlparse(addr).scheme}://{urlparse(addr).netloc}"
        #Internal links
        url_links = url_links[url_links['Destination'].str.startswith(domain)]

        broken = list(set(url_links[~url_links['Status Code'].isin(STATUS_20X+STATUS_30X)]['Destination'].values))
        redirected = list(set(url_links[url_links['Status Code'].isin(STATUS_30X)]['Destination'].values))

        df.loc[index, 'Issue: Broken Links'] = "\n".join(broken)
        df.loc[index, 'Issue: Redirecting Links'] = "\n".join(redirected)

    return df 

def save_issues_to_file(df, filename="internal_html_with_issues.csv"):
    """Saves the dataframe to an excel file"""
    df.to_csv(filename, index=False)
