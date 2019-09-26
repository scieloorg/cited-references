#!/usr/bin/env python3
import sys
import time

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select

start_pos = sys.argv[1]
last_pos = 7900

res_file_path = '/home/rafael/ids_ulrich_' + start_pos + '.txt'
chromeDriver_path = '/home/rafael/Downloads/chromedriver'

res_file = open(res_file_path, "w+")
res_file.close()

driver = webdriver.Chrome(chromeDriver_path)
driver.get('http://ulrichsweb.serialssolutions.com/')

time.sleep(5)

driver.find_element_by_id('submitSearchTop').click()

time.sleep(5)

dropdown = Select(driver.find_element_by_class_name('ui-pg-selbox'))
dropdown.select_by_value('100')

time.sleep(5)

for i in range(4):
    driver.find_element_by_class_name('ui-pg-input').send_keys(Keys.BACKSPACE)
driver.find_element_by_class_name('ui-pg-input').send_keys(start_pos)
time.sleep(2)
driver.find_element_by_class_name('ui-pg-input').send_keys(Keys.ENTER)
time.sleep(5)

if int(start_pos) + 100 < last_pos:
    last_pos = int(start_pos) + 1000
for i in range(int(start_pos), last_pos):
    max_tries = 5
    collected = False
    while not collected and max_tries > 0:
        try:
            print('try %d of page %d' % (5 - max_tries + 1, i))
            links = driver.find_elements_by_class_name("titleDetailsLink")
            for l in links:
                res_file = open(res_file_path, 'a')
                res_file.write(l.get_attribute('href') + '\n')
            res_file.close()
            driver.find_elements_by_id('next_t')[0].click()
            time.sleep(4)
            collected = True
        except:
            max_tries -= 1
            print('page %s failed' % i)
            time.sleep(5)

driver.quit()
