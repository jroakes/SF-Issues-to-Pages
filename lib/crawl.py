import glob, os, re
import subprocess
from config import SF_CONFIG_PATH

from tqdm.auto import tqdm


def crawler(url, output_folder = None):
        
    #Get Screaming Frog Program Files Path
    search_paths = ['C:\Program Files\Screaming Frog SEO Spider', 'C:\Program Files (x86)\Screaming Frog SEO Spider', '~/.ScreamingFrogSEOSpider/']
    for path in search_paths:
        # If the path exists, save to sf_path variable and break the loop
        if os.path.exists(path):
            sf_path = path
            break
    # If the path doesn't exist, print an error message and exit
    else:
        print("Error: Screaming Frog SEO Spider not found.")
        return False


    #Get full path. If output folder doesn't exist, create it and get full path
    if output_folder is None:
        ## Create data folder if it doesn't exist
        if not os.path.exists('data'):
            os.makedirs('data')
        
        output_folder = os.path.abspath('data')

    elif not os.path.exists(output_folder):
        print('Output folder does not exist.')
        return False
    
    else:
        output_folder = os.path.abspath(output_folder)

    sf_executable = f"{sf_path}\ScreamingFrogSEOSpiderCli"

    command = f""""{sf_executable}" --crawl {url} -headless --save-crawl --output-folder "{output_folder}" --config "{SF_CONFIG_PATH}" --export-tabs "Internal:HTML" --bulk-export "Issues:All,Links:All Inlinks,Images:All Image Inlinks" --export-format csv"""

    #Execute Screaming Frog Crawl
    stream = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

    #Wait for Screaming Frog to finish and ouput progress with tqdm
    pbar = tqdm(total=100, desc="Crawling")

    while stream.poll() is None:
        line = stream.stdout.readline().strip()
        if 'mCompleted' in line:
            pct_completed = re.search(r'(?:mCompleted=)([\d\.]+)(?=\%)', line).group(1)
            num_completed = re.search(r'mCompleted=(\d+)(?!\%)', line).group(1)
            num_waiting = re.search(r'mWaiting=(\d+)', line).group(1)
            pbar.set_description(f"Crawling {num_completed} pages, {num_waiting} waiting")
            pbar.update(int(float(pct_completed)) - pbar.n)

    pbar.close()
    stream.wait()

    return True