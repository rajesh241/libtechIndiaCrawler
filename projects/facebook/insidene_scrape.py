"""Blank file which can server as starting point for writing any script file"""
import argparse
import requests
import json
from bs4 import BeautifulSoup
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.generic.libtech_queue import libtech_queue_manager

def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-c', '--crawl', help='Crawl',
                        required=False, action='store_const', const=1)
    parser.add_argument('-d', '--download', help='Download',
                        required=False, action='store_const', const=1)
    parser.add_argument('-ctg', '--category', help='Category to be crawled', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args

def download_all_posts(logger, category):
    job_list = []
    headers = get_headers()
    func_name = "parse_save_insidene"
    infile = f"data/json/insidene/{category}.json"
    filepath = f"data/json/insidene/{category}"
    with open(infile) as data_file:
        post_data_dict = json.load(data_file)
    for page_no, page_dict in post_data_dict.items():
        logger.info(page_no)
        for article_no, article_dict in page_dict.items():
            filename = f"{filepath}/{page_no}_{article_no}.json"
            logger.info(filename)
            func_args = [headers, filename, article_dict]
            job_dict = {
                'func_name' : func_name,
                'func_args' : func_args
            }
            job_list.append(job_dict)
    logger.info(job_list[:1])
    libtech_queue_manager(logger, job_list, num_threads=20)

def delete_divs_by_classes(logger, mysoup, class_array):
    logger.info(class_array)
    for my_class in class_array:
        my_div = mysoup.find("div", attrs={"class" : my_class})
        if my_div is not None:
            my_div.decompose()
        logger.info(my_class)
    return mysoup
  #for class in class_array:
  #    my_div = mysoup.find("div", attrs={"class" : class})
  #    if my_div is not None:
  #        my_div.decompose()
  #return mysoup
def parse_page(logger, url):
    headers = get_headers()
    logger.info(url)
    post_div = None
    post_content = ''
    response = requests.get(url, headers=headers)
    logger.info(response.status_code)
    if response.status_code == 200:
        mysoup = BeautifulSoup(response.content, "lxml")
        post_div = mysoup.find("div", attrs={"class" : "td-post-content"})
    if post_div is not None:
        class_array = ["code-block", "google-auto-placed",
                       "addtoany_share_save_container",
                       "jp-relatedposts"]
        post_div = delete_divs_by_classes(logger, post_div, class_array)
        code_div = post_div.find("div", attrs={"class" : "code-block"})
        strong_p = post_div.findAll("strong")
        for strong in strong_p:
            strong_text = strong.text.lstrip().rstrip()
            strong_a = strong.find("a")
            if ("ALSO READ:" in strong_text) and (strong_a is not None):
                parent_p = strong.parent
                parent_p.decompose()
        em_p = post_div.findAll("em")
        for strong in em_p:
            strong_text = strong.text.lstrip().rstrip()
            if ("Support Inside Northeast" in strong_text):
                parent_p = strong.parent
                parent_p.decompose()


        paras = post_div.findAll("p")
        for para in paras:
            para_text = para.text.lstrip().rstrip()
            post_content = f"{post_content}{para_text}\n"
    logger.info(post_content)


def get_total_pages(logger, mysoup):
    total_pages = 0
    page_nav_div = mysoup.find("div", attrs={"class" : "page-nav"})
    with open("data/temp/a.html", "w") as f:
        f.write(mysoup.prettify())
    if page_nav_div is not None:
        last_a = page_nav_div.find("a", attrs={"class" : "last"})
        if last_a is not None:
            total_pages = last_a.text.lstrip().rstrip()
    logger.info(total_pages)
    return total_pages


def scrape_posts(logger, mysoup):
    posts = {}
    content_divs = mysoup.findAll("div", attrs={"class" : "td_module_10"})
    i = 0
    for content_div in content_divs:
        i = i + 1
        title = ''
        post_content = ''
        post_date = ''
        title_link = ''
        h3 = content_div.find("h3", attrs={"class" : "entry-title"})
        if h3 is not None:
            title = h3.text.lstrip().rstrip()
            h3_a = h3.find("a")
            if h3_a is not None:
                title_link = h3_a["href"]
        post_date_span = content_div.find("span", attrs={"class" : "td-post-date"})
        if post_date_span is not None:
            post_date = post_date_span.text.lstrip().rstrip()
        post_content_div = content_div.find("div", attrs={"class" : "td-excerpt"})
        if post_content_div is not None:
            post_content = post_content_div.text.lstrip().rstrip()
        d = {}
        d['post_title'] = title
        d['post_link'] = title_link
        d['post_date'] = post_date
        d['post_content'] = post_content
        posts[i] = d
        
    return posts

def get_headers():
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:72.0) Gecko/20100101 Firefox/72.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
     }
    return headers

def scrape_ne(logger, category):
    logger.info("Going to scrapw the ne magazine")
    posts = {}
    filename = f"{category}.json"
    url = "https://insidene.com"
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:72.0) Gecko/20100101 Firefox/72.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
     }
    r = requests.get(url, headers=headers)
    logger.info(r.status_code)
    cookies = r.cookies
    base_url = f"https://insidene.com/category/{category}/"
    logger.info(base_url)
    r = requests.get(base_url, headers=headers, cookies=cookies)
    logger.info(r.status_code)
    total_pages = 0
    if r.status_code == 200:
        myhtml = r.content
        mysoup = BeautifulSoup(myhtml, "lxml")
        total_pages = get_total_pages(logger, mysoup)
    logger.info(f"Total pages is {total_pages}")
    for i in range(1, int(total_pages)+1):
        logger.info(i)
        page_posts = {}
        url = f"{base_url}page/{i}/"
        r = requests.get(url, headers=headers, cookies=cookies)
        if r.status_code == 200:
            mysoup = BeautifulSoup(r.content, "lxml")
            page_posts = scrape_posts(logger, mysoup)
        posts[i] = page_posts
    save_data(posts, filename)


def save_data(data, filename):
    """Converts data to JSON.
    """
    with open(f'data/json/insidene/{filename}', 'w') as json_file:
        json.dump(data, json_file, indent=4)
def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.info("Testing phase")
        test_input = args['testInput1']
        parse_page(logger, test_input)
    if args['download']:
        logger.info("Downloading NE Magazine")
        category = args['category']
        if category is not None:
            download_all_posts(logger, category)

    if args['crawl']:
        logger.info("Crawling NE Magazine")
        category = args['category']
        if category is not None:
            scrape_ne(logger, category)
    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
