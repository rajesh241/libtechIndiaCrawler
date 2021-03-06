"""Blank file which can server as starting point for writing any script file"""
from os import listdir
from os.path import isfile, join
import argparse
import requests
from libtech_lib.generic.commons import logger_fetch
import re
import json
import time
import logging
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup

def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-c', '--crawl', help='Crawl Facebook',
                        required=False, action='store_const', const=1)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-id', '--inputDir', help='Input directory for HTML', required=False)
    parser.add_argument('-od', '--outputDir', help='Output Directory', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args
def get_bs(session, url, filename=None):
    """Makes a GET requests using the given Session object
    and returns a BeautifulSoup object.
    """
    r = None
    while True:
        r = session.get(url)
        if r.ok:
            break
    mysoup = BeautifulSoup(r.content, "lxml")
    if filename is not None:
        with open(filename, "w") as f:
            f.write(mysoup.prettify())
    return mysoup


def make_login(session, base_url, credentials):
    """Returns a Session object logged in with credentials.
    """
    login_form_url = '/login/device-based/regular/login/?refsrc=https%3A'\
        '%2F%2Fmobile.facebook.com%2Flogin%2Fdevice-based%2Fedit-user%2F&lwv=100'

    params = {'email':credentials['email'], 'pass':credentials['pass']}

    while True:
        time.sleep(3)
        logged_request = session.post(base_url+login_form_url, data=params)
        
        if logged_request.ok:
            logging.info('[*] Logged in.')
            break

def get_post_id(profile_bs):
        try:
            posts_id = 'recent'
            posts = profile_bs.find('div', id=posts_id).div.div.contents
        except Exception:
            posts_id = 'structured_composer_async_container'
            posts = profile_bs.find('div', id=posts_id).div.div.contents
        return posts_id

def get_posts(logger, number_of_posts):
    a = []
    posts_urls = [a['href'] for a in profile_bs.find_all('a', text='Full Story')] 
    a= a+posts_urls
    current_number_posts = len(a)
    logger.info(f"posts urls {len(posts_urls)}")
    posts_id = get_post_id(profile_bs)
    logger.info(f"Post ID is {posts_id}")
    show_more_posts_url = profile_bs.find('div', id=posts_id).next_sibling.a['href']
    logger.info(f"show more posts url is {show_more_posts_url}")
    profile_bs = get_bs(session, base_url+show_more_posts_url)
    posts_urls = [a['href'] for a in profile_bs.find_all('a', text='Full Story')] 
 
def crawl_profile(logger, session, base_url, profile_url, post_limit):
    """Goes to profile URL, crawls it and extracts posts URLs.
    """
    filename = "profile.html"
    profile_bs = get_bs(session, profile_url, filename=filename)
    n_scraped_posts = 0
    scraped_posts = list()
    posts_id = None
    a = []
    posts_urls = [a['href'] for a in profile_bs.find_all('a', text='Full Story')] 
    a= a+posts_urls
    logger.info(f"posts urls {len(posts_urls)}")
    posts_id = get_post_id(profile_bs)
    logger.info(f"Post ID is {posts_id}")
    show_more_posts_url = profile_bs.find('div', id=posts_id).next_sibling.a['href']
    logger.info(f"show more posts url is {show_more_posts_url}")
    profile_bs = get_bs(session, base_url+show_more_posts_url)
    posts_urls = [a['href'] for a in profile_bs.find_all('a', text='Full Story')] 
    logger.info(f"posts urls {len(posts_urls)}")
    a= a+posts_urls
    posts_urls = a
    logger.info(f"posts urls {len(posts_urls)}")
    for post_url in posts_urls:
       logger.info(f"Currentply processing {post_url}")
       try:
           post_data = scrape_post(session, base_url, post_url)
           logger.info(f"Scraped data is {post_data}")
           scraped_posts.append(post_data)
       except Exception as e:
           logging.info('Error: {}'.format(e))
       n_scraped_posts += 1
    return scraped_posts
    
    exit(0)
    while n_scraped_posts < post_limit:
        try:
            posts_id = 'recent'
            posts = profile_bs.find('div', id=posts_id).div.div.contents
        except Exception:
            posts_id = 'structured_composer_async_container'
            posts = profile_bs.find('div', id=posts_id).div.div.contents
        logger.info(f"posts is {posts}")
        posts_urls = [a['href'] for a in profile_bs.find_all('a', text='Full Story')] 
        logger.info(f"posts urls {posts_urls}")
        for post_url in posts_urls:
            logger.info(f"Currentply processing {post_url}")
            try:
                post_data = scrape_post(session, base_url, post_url)
                logger.info(f"Scraped data is {post_data}")
                scraped_posts.append(post_data)
            except Exception as e:
                logging.info('Error: {}'.format(e))
            n_scraped_posts += 1
            if posts_completed(scraped_posts, post_limit):
                break
        
        show_more_posts_url = None
        if not posts_completed(scraped_posts, post_limit):
            try:
                show_more_posts_url = profile_bs.find('div', id=posts_id).next_sibling.a['href']
                profile_bs = get_bs(session, base_url+show_more_posts_url)
                time.sleep(3)
            except:
                a="well cone"
        else:
            break
    return scraped_posts

def posts_completed(scraped_posts, limit):
    """Returns true if the amount of posts scraped from
    profile has reached its limit.
    """
    if len(scraped_posts) == limit:
        return True
    else:
        return False


def scrape_post(session, base_url, post_url):
    """Goes to post URL and extracts post data.
    """
    post_data = OrderedDict()

    post_bs = get_bs(session, base_url+post_url)
    time.sleep(5)

    # Here we populate the OrderedDict object
    post_data['url'] = post_url

    try:
        post_text_element = post_bs.find('div', id='u_0_0').div
        string_groups = [p.strings for p in post_text_element.find_all('p')]
        strings = [repr(string) for group in string_groups for string in group]
        post_data['text'] = strings
    except Exception:
        post_data['text'] = []
    
    try:
        post_data['media_url'] = post_bs.find('div', id='u_0_0').find('a')['href']
    except Exception:
        post_data['media_url'] = ''
    

    try:
        post_data['comments'] = extract_comments(session, base_url, post_bs, post_url)
    except Exception:
        post_data['comments'] = []
    
    return dict(post_data)


def extract_comments(session, base_url, post_bs, post_url):
    """Extracts all coments from post
    """
    comments = list()
    show_more_url = post_bs.find('a', href=re.compile('/story\.php\?story'))['href']
    first_comment_page = True

    logging.info('Scraping comments from {}'.format(post_url))
    while True:

        logging.info('[!] Scraping comments.')
        time.sleep(3)
        if first_comment_page:
            first_comment_page = False
        else:
            post_bs = get_bs(session, base_url+show_more_url)
            time.sleep(3)
        
        try:
            comments_elements = post_bs.find('div', id=re.compile('composer')).next_sibling\
                .find_all('div', id=re.compile('^\d+'))
        except Exception:
            pass

        if len(comments_elements) != 0:
            logging.info('[!] There are comments.')
        else:
            break
        
        for comment in comments_elements:
            comment_data = OrderedDict()
            comment_data['text'] = list()
            try:
                comment_strings = comment.find('h3').next_sibling.strings
                for string in comment_strings:
                    comment_data['text'].append(string)
            except Exception:
                pass
            
            try:
                media = comment.find('h3').next_sibling.next_sibling.children
                if media is not None:
                    for element in media:
                        comment_data['media_url'] = element['src']
                else:
                    comment_data['media_url'] = ''
            except Exception:
                pass
            
            comment_data['profile_name'] = comment.find('h3').a.string
            comment_data['profile_url'] = comment.find('h3').a['href'].split('?')[0]
            comments.append(dict(comment_data))
        
        show_more_url = post_bs.find('a', href=re.compile('/story\.php\?story'))
        if 'View more' in show_more_url.text:
            logging.info('[!] More comments.')
            show_more_url = show_more_url['href']
        else:
            break
    
    return comments


def json_to_obj(filename):
    """Extracts dta from JSON file and saves it on Python object
    """
    obj = None
    with open(filename) as json_file:
        obj = json.loads(json_file.read())
    return obj


def save_data(data):
    """Converts data to JSON.
    """
    with open('profile_posts_data.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
def process_text(s):
    """Process the text to remove new lines and conver multiple spaces to
    single space"""
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")
    s = re.sub('\s+',' ',s)
    return s

def extract_posts(logger, mysoup):
    posts_dict = {}
    #post_divs = mysoup.findAll("div", data-testid="post_message")
    post_divs = mysoup.findAll("div", attrs={"class":"userContentWrapper"})
    post_divs = mysoup.findAll("div", attrs={"data-fte":"1"})
    ##Comment div = data-testid="UFI2Comment/body
    ##Comment Count = data-testid="UFI2CommentsCount/root
    ##share count = data-testid="UFI2SharesCount/root"
    sr_no = 0
    csv_array = []
    csv_headers = ["post_date", "post_content"]
    i = 0
    for post_div in post_divs:
        p = {}
        post_data = ''
        date_string = ''
        likes_string = ''
        comments_string = ''
        shares_string = ''
        comments_a = None
        shares_a = None
        sr_no = sr_no + 1
        logger.info(f"serial number is {sr_no}")
        post_data_div = post_div.find("div",
                                         attrs={"data-testid":"post_message"})
        if post_data_div is not None:
            post_data = post_data_div.text.lstrip().rstrip().replace("\n"," ")
        
        meta_data_div = post_div.find("div",
                                         attrs={"data-testid":"story-subtitle"})
        if meta_data_div is not None:
            date_span = meta_data_div.find("span", attrs={"class":"timestampContent"})
            date_string = date_span.text.lstrip().rstrip()

        comments_a = post_div.find("a",
                                    attrs={"data-testid":"UFI2CommentsCount/root"}) 
        if comments_a is not None:
            comments_string = comments_a.text.lstrip().rstrip()
        shares_a = post_div.find("a",
                                 attrs={"data-testid":"UFI2SharesCount/root"}) 
        if shares_a is not None:
            shares_string = shares_a.text.lstrip().rstrip()
        p['post_date'] = date_string
        p['post_content'] = process_text(post_data)
        p['comment_count'] = comments_string
        p['share_count'] = shares_string
        logger.info(date_string)
        logger.info(comments_string)
        logger.info(shares_string)
        comment_dict = {}
        comment_divs = post_div.findAll("div",
                                      attrs={"data-testid":"UFI2Comment/body"})
        comment_no = 0
        for comment_div in comment_divs:
            comment_no = comment_no + 1
            comment_span_div = comment_div.find("span",
                                                attrs={"dir":"ltr"})
            if comment_span_div is not None:
                comment_text = comment_span_div.text.lstrip().rstrip()
                comment_dict[comment_no] = process_text(comment_text)

        p['comments'] = comment_dict
        logger.info(p)
        posts_dict[sr_no] = p
    return posts_dict
    #with open('profile_posts_data.json', 'w', encoding='utf8') as json_file:
    #    json.dump(posts_dict, json_file, indent=4, ensure_ascii=False)


def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        input_dir = args['inputDir']
        if input_dir is None:
            input_dir = "data/profiles/"
        output_dir = args['outputDir']
        if output_dir is None:
            output_dir = "data/json/"
        file_list = [f for f in listdir(input_dir) if isfile(join(input_dir, f))]
        logger.info(file_list)
        posts_dict = {}
        for each_file in file_list:
            filename = f"{input_dir}{each_file}"
            with open(filename, "rb") as f:
                myhtml = f.read()
            mysoup = BeautifulSoup(myhtml, "lxml")
            my_dict = extract_posts(logger, mysoup)
            posts_dict[each_file] = my_dict
        with open(f'{output_dir}profile_posts_data.json', 'w', encoding='utf8') as json_file:
            json.dump(posts_dict, json_file, indent=4, ensure_ascii=False)
        exit(0)
        logger.info("Testing phase")
        with open("himanta_cleaned.html", "rb") as f:
            myhtml = f.read()
        mysoup = BeautifulSoup(myhtml, "lxml")
        extract_posts(logger, mysoup)
        exit(0)
        
        with open("himanta.html", "rb") as f:
            myhtml = f.read()
        mysoup = BeautifulSoup(myhtml, "lxml")
        with open("himanta_cleaned.html", "w") as f:
            f.write(mysoup.prettify())
    if args['crawl']:
        logger.info("Crawling facebook")
        base_url = 'https://mobile.facebook.com'
        session = requests.session()

        # Extracts credentials for the login and all of the profiles URL to scrape
        credentials = json_to_obj('credentials.json')
        profiles_urls = json_to_obj('profiles_urls.json')

        make_login(session, base_url, credentials)
        posts_data = None
        for profile_url in profiles_urls:
            posts_data = crawl_profile(logger, session, base_url, profile_url, 25)
            logging.info('[!] Scraping finished. Total: {}'.format(len(posts_data)))
            logging.info('[!] Saving.')
            save_data(posts_data)

    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
