from urllib.request import urlopen, Request
import urllib.error
from bs4 import BeautifulSoup, NavigableString, Tag
import pandas as pd 
import numpy as np 
from tqdm import tqdm
from itertools import repeat
import multiprocessing

project_path = '/Users/IvanK/FastFashion'

def parse_newsletter(url, content_kwargs = {}):
    '''
    This function collects all the relevant information from a specific marketting newsletter and outputs it
    as a dict. For urls that do not exists it outputs an empty dict and a different exit code.
    '''
    
    output_placeholder = {}
    exit_code          = 1 # success of our prodcedure
    
    # form a request
    headers = {'User-Agent': 'Mozilla/5.0'}
    try: 
        req     = Request(url = url, headers = headers)
        page    = urlopen(req).read()
        soup    = BeautifulSoup(page, 'html.parser')
        
        # assuming that the page format is the same
        title       = soup('h1')[0].string
        txt_content = soup('h2')[0].string    

        html_time_candidate = soup('time')[0]
        
        if html_time_candidate['data-format'] == '%B %e, %Y %l:%M%P':

            time_sent = soup('time')[0].string
        
        else:

            time_sent = 'NaN'

        output_placeholder['title']         = title
        output_placeholder['txt_content']   = txt_content
        output_placeholder['time_sent']     = time_sent
    
    except urllib.error.HTTPError:
        print(url)
        exit_code = 0

    return output_placeholder, exit_code

# tested on 
# url = "https://milled.com/Hermes/a-forever-kind-of-thing-8YXBf3RR1HBeEkaL"
# newsletter_test = parse_newsletter(url)

def get_all_newsletter_urls_for_brand(brand_name, brand_name_url_dict, years):
    '''
    This function opens the main page of a brand on milled.com (which is stored in @brand_name_url_dict) and
    collects all the urls of individual campaings within years in the list @years and outputs it as a list
    '''
    
    results_placeholder = []
    brand_url           = brand_name_url_dict[brand_name]
    headers             = {'User-Agent': 'Mozilla/5.0'}
    
    for year in years:

        year_brand_url = brand_url + f'/{year}'

        # try to find all newsletter links for the specified year 
        req  = Request(url = year_brand_url, headers = headers)
        page = urlopen(req).read()
        soup = BeautifulSoup(page, features="lxml")

        year_links = [cand.get('href', 'no_such_tag') for cand in soup.findAll('a', attrs={'class':"line-clamp-3"})]
        year_links = ['https://milled.com' + link for link in year_links if link != 'no_such_tag']

        if len(year_links) > 0:
            results_placeholder += year_links

        else:
            pass

    return results_placeholder

# tested on 
# brand_name_url_dict = {'wearPACT' : "https://milled.com/wearPACT"}
# years = [i for i in range(2012,2024)]
# years = [str(i) for i in years]
# years = ['2014']
# test_res = get_all_newsletter_urls_for_brand('wearPACT', brand_name_url_dict, years)
# len(test_res) == 23            

def get_all_newsletter_data_for_brand_from_urls(brand_newsletter_urls, brand_name, brand_name_fast_fash_dict):
    '''
    This function takes a list of marketing urls for a brand and parses basic info (defined further)
    from each newsletter and saves it to a dataframe. I also look up whether the brand is fast fashion and
    save it in the df as well.
    '''

    if len(brand_newsletter_urls) > 0:

        # placeholder for information from each newsletter
        brand_titles      = []
        brand_txt_content = []
        brand_times_sent  = []
        brand_urls        = []

        for newsletter_url in brand_newsletter_urls:

            newsletter_info, exit_code = parse_newsletter(newsletter_url)

            if exit_code == 1: # save newsletter data only when the url was successfully opened

                # this part can be simplified by doing smth like that to avoid append lines for each column, where pg_L is a list of dicts
                # combined_d = {}
                # keys_d = list(pg_L[0].keys())
                # for k in keys_d:
                #     combined_d[k] = []
                # for d in pg_L:
                #     for k in keys_d:
                #         combined_d[k] += [d[k]]

                brand_titles.append(newsletter_info['title'])
                brand_txt_content.append(newsletter_info['txt_content'])
                brand_times_sent.append(newsletter_info['time_sent'])
                brand_urls.append(newsletter_url)

        brand_df = pd.DataFrame( data = {'title': brand_titles, 'txt_content': brand_txt_content,
                                'time_sent': brand_times_sent, 'url' : brand_urls}
                                )

        brand_df['brand_name']      = brand_name
        brand_df['is_fast_fashion'] = brand_name_fast_fash_dict[brand_name]
    
    # FIXME what happens if there all urls give exit code 0 ???

    else: # initialise an empty df if there are no newsletters

        brand_titles      = [np.nan]
        brand_txt_content = [np.nan]
        brand_times_sent  = [np.nan]
        brand_urls        = [np.nan]

        brand_df = pd.DataFrame( data = {'title': brand_titles, 'txt_content': brand_txt_content,
                                'time_sent': brand_times_sent, 'url' : brand_urls}
                                )

        brand_df['brand_name']      = brand_name
        brand_df['is_fast_fashion'] = brand_name_fast_fash_dict[brand_name]
            
    return brand_df


def run(year_L : int, year_H: int):
    '''
    This function collects all marketing newsletters from milled.com for
    a list of brands specified in BrandListFiltered for years in [year_L, year_H]
    interval.
    The function is quite slow and takes about an hour for one year. It is slow because
    each brand has 1000s of marketing campaigns in most years and the code has to follow each link within a loop.

    Paralelisng the query of newsletter links should help but on my machine this did not work so well because
    bs4 and multiprocessing create some recursion depth issues. FIXME I should find a way to fix this.
    '''

    # brand_list = pd.read_excel('../Data/BrandList.xlsx', sheet_name= 'Sheet2')
    # brand_list.to_csv('../Data/BrandListFiltered.csv', index = False)
    
    # initialise important elements for this run

    brand_list                = pd.read_csv(f'{project_path}/Data/BrandListFiltered.csv')
    brand_names               = list(brand_list['BrandName'].unique())
    brand_name_url_dict       = dict(zip(brand_list['BrandName'].values, brand_list['Url'].values))
    brand_name_fast_fash_dict = dict(zip(brand_list['BrandName'].values, brand_list['isFastFashion'].values))
    
    years = [i for i in range(year_L,year_H + 1)]
    years = [str(i) for i in years]

    cpus = 12

    # FIXME what happens if years are not present on the website???

    # FIXME need to rework it. Initially, I was not planning that running the code would take so long
    # but even for one year it takes considerable amount of time so I decided to run it for each year separately.
    # while the code was initially designed to take a list of years and be run for all years,
    # so changing @get_all_newsletter_urls_for_brand to take a single year instead of a list is reasonable
    output_df_placeholder = []
    for year in years:
        year_to_list = [year]


        # get all newsletter links for each brand    
        with multiprocessing.Pool(cpus) as pool:

            all_newsletters = pool.starmap(get_all_newsletter_urls_for_brand, zip(brand_names, repeat(brand_name_url_dict), repeat(year_to_list))) 
        
        all_newsletters_d = {}
        print('Got all the Links!')

        for brand_name, brand_urls in zip(brand_names, all_newsletters):
            all_newsletters_d[brand_name] = brand_urls

        # get information about each newsletter for each brand
        results_placeholder = []
        for brand_name in tqdm(brand_names): # this can be easily parallelised

            brand_newsletter_urls = all_newsletters_d[brand_name]

            brand_df = get_all_newsletter_data_for_brand_from_urls(brand_newsletter_urls, brand_name, brand_name_fast_fash_dict)
            results_placeholder.append(brand_df)
    
        year_output_df = pd.concat(results_placeholder) # merge dataframes of each brand into a single one
        year_output_df['year'] = year

        output_df_placeholder.append(year_output_df)
    
    output_df = pd.concat(output_df_placeholder)
    output_df = output_df.reset_index(drop = True)

    output_df.to_csv(f'{project_path}/Data/Newsletters_from_{year_L}_to_{year_H}.csv', index = False)

    return output_df
    
 


