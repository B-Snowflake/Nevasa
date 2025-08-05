#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/12/28
import json
import time
import traceback
from datetime import datetime
import platform
import subprocess
import uuid
import ntplib
import xmltodict
import requests


class License:
    def __init__(self, server_url: str = None):
        if server_url is None:
            self.server_mode = False
        else:
            self.server_mode = True
            self.server_url = server_url
            self.encrypts_dict_try_times, self.current_date_try_times = 0, 0
        self.client = ntplib.NTPClient()
        self.encrypts_dict, self.start_site_list, self.intervening_sequence_list, self.end_site_list, self.current_date = None, None, None, None, None
        self.get_current_date()
        self.machine_id = self.get_machine_id()
        self.min_insertions, self.max_insertions = 0, 3
        if not self.server_mode:
            self.encrypts_dict_1 = {
                '00': '7',
                '01': 's',
                '02': 'k',
                '03': 'p',
                '04': 'u',
                '05': 'z',
                '06': '1',
                '07': 'q',
                '08': 'y',
                '09': 'e',
                '10': '5',
                '11': 'i',
                '12': 'r',
                '13': 'w',
                '14': 'd',
                '15': 'm',
                '16': '3',
                '17': '4',
                '18': 'o',
                '19': 'g',
                '20': '0',
                '21': 'n',
                '22': 'c',
                '23': 'h',
                '24': '9',
                '25': 'f',
                '26': '8',
                '27': '6',
                '28': 'v',
                '29': 'j',
                '30': 'a',
                '31': 'l',
                '32': 't',
                '33': '2',
                '34': 'b',
                '35': 'x',
                '36': '0',
                '37': '0',
                '38': '2',
                '39': '3',
                '41': '8',
                '42': '2',
                '43': 'q',
                '44': '8',
                '46': '9',
                '47': 'v',
                '48': '0',
                '49': 'n',
                '50': '9',
                '51': '6',
                '52': '2',
                '53': '6',
                '54': '5',
                '55': 'c',
                '56': 'z',
                '57': 'x',
                '60': '7',
                '61': 'v',
                '63': '7',
                '64': '3',
                '65': 't',
                '66': 'h',
                '67': '1',
                '69': '5',
                '70': '4',
                '71': 'v',
                '73': 'u',
                '74': 'o',
                '75': '8',
                '76': '6',
                '77': '9',
                '78': 'o',
                '80': '6',
                '82': 'b',
                '83': '6',
                '84': 'l',
                '85': 'l',
                '86': 's',
                '87': 'w',
                '89': '6',
                '91': '2',
                '92': 'm',
                '93': '6',
                '94': '8',
                '95': '6',
                '96': '2',
                '97': '0',
                '98': 'g',
            }
            self.encrypts_dict_2 = {
                '00': '5',
                '01': 'l',
                '02': 'v',
                '03': 'a',
                '04': 'e',
                '05': 'i',
                '06': '2',
                '07': 'k',
                '08': 'q',
                '09': 'o',
                '10': 'u',
                '11': '0',
                '12': 'd',
                '13': 'x',
                '14': '6',
                '15': 'f',
                '16': 'w',
                '17': 'n',
                '18': 'c',
                '19': '9',
                '20': 'h',
                '21': 'j',
                '22': 'z',
                '23': '8',
                '24': 'b',
                '25': 'm',
                '26': '7',
                '27': '3',
                '28': 's',
                '29': 't',
                '30': 'y',
                '31': '4',
                '32': 'g',
                '33': '1',
                '34': 'p',
                '35': 'r',
                '36': '0',
                '37': 'v',
                '38': 'p',
                '39': 'k',
                '41': '0',
                '42': '9',
                '43': 's',
                '44': '0',
                '46': '4',
                '47': '6',
                '48': '8',
                '49': '3',
                '50': 'p',
                '51': 'j',
                '52': 'g',
                '53': 'x',
                '54': '6',
                '55': 'l',
                '56': 'e',
                '57': 'q',
                '60': '2',
                '61': '0',
                '63': 'm',
                '64': '2',
                '65': '3',
                '66': 'n',
                '67': 'p',
                '69': 'p',
                '70': '0',
                '71': '5',
                '73': '2',
                '74': 'i',
                '75': 'z',
                '76': '4',
                '77': 'h',
                '78': 'i',
                '80': 'n',
                '82': '1',
                '83': 'h',
                '84': '3',
                '85': '6',
                '86': 'q',
                '87': 'q',
                '89': '9',
                '91': '1',
                '92': '9',
                '93': '8',
                '94': '2',
                '95': '1',
                '96': 'c',
                '97': '0',
                '98': 'h',
            }
            self.encrypts_dict_3 = {
                '00': '5',
                '01': '8',
                '02': '7',
                '03': 'p',
                '04': 'j',
                '05': '1',
                '06': 'b',
                '07': 'w',
                '08': '4',
                '09': '6',
                '10': 'u',
                '11': 'e',
                '12': '2',
                '13': 't',
                '14': 'n',
                '15': 'o',
                '16': 'a',
                '17': 'd',
                '18': '0',
                '19': 'c',
                '20': '9',
                '21': '3',
                '22': 'h',
                '23': 'g',
                '24': 'r',
                '25': 'z',
                '26': 'y',
                '27': 'l',
                '28': 'q',
                '29': 'f',
                '30': 'v',
                '31': 'k',
                '32': 'm',
                '33': 's',
                '34': 'x',
                '35': 'i',
                '36': '0',
                '37': '9',
                '38': '4',
                '39': 'e',
                '41': 't',
                '42': '1',
                '43': 'w',
                '44': '3',
                '46': '8',
                '47': 'z',
                '48': '5',
                '49': '8',
                '50': 'o',
                '51': 's',
                '52': '8',
                '53': '9',
                '54': 'm',
                '55': '8',
                '56': '7',
                '57': 'q',
                '60': 'v',
                '61': '2',
                '63': 'q',
                '64': '0',
                '65': '9',
                '66': 'd',
                '67': 'w',
                '69': 't',
                '70': '3',
                '71': '3',
                '73': '8',
                '74': 'u',
                '75': 'j',
                '76': 'z',
                '77': '8',
                '78': 'g',
                '80': 'o',
                '82': '9',
                '83': 't',
                '84': '4',
                '85': 'e',
                '86': 'i',
                '87': '9',
                '89': '2',
                '91': '8',
                '92': 'x',
                '93': '1',
                '94': '0',
                '95': '6',
                '96': 'n',
                '97': 'd',
                '98': '0',
            }
            self.offline_encrypts_dict = {'too': self.encrypts_dict_1, 'yme': self.encrypts_dict_2, 'anl': self.encrypts_dict_3}
            self.start_site_list = ['45', '62', '58', '79', ]
            self.intervening_sequence_list = ['59', '88', '68', '81', '90']
            self.end_site_list = ['99', '40', '72']
            self.unuseable_list = []
            for element in self.start_site_list + self.intervening_sequence_list + self.end_site_list:
                new_element = 99 - int(element)
                self.unuseable_list.append(f"{new_element:02}")
        else:
            self.get_encrypts_dict()

    def get_encrypts_dict(self):
        while self.encrypts_dict is None or self.start_site_list is None or self.intervening_sequence_list is None or self.end_site_list is None:
            try:
                response = requests.request(method='GET', url=f'{self.server_url}/api/get_encrypts_dict?machine_id={self.machine_id}')
                if json.loads(response.text)['message'] != 'machine_id not found':
                    self.encrypts_dict, self.start_site_list, self.intervening_sequence_list, self.end_site_list = json.loads(response.text)['encrypts_dict']
                    self.unuseable_list = []
                    for element in self.start_site_list + self.intervening_sequence_list + self.end_site_list:
                        new_element = 99 - int(element)
                        self.unuseable_list.append(f"{new_element:02}")
                else:
                    self.encrypts_dict, self.start_site_list, self.intervening_sequence_list, self.end_site_list = {}, [], [], []
            except Exception as e:
                self.encrypts_dict, self.start_site_list, self.intervening_sequence_list, self.end_site_list = None, None, None, None
                self.encrypts_dict_try_times += 1
                print('fail to get encrypts dict')
                if self.encrypts_dict_try_times > 3:
                    self.encrypts_dict_try_times = 0
                    raise Exception('network error,cannot get dict')
                time.sleep(0.1)

    def write_computer_info(self, file):
        info = {}
        computer_info = {
            'hostname': platform.node(),
            'platform': platform.platform(),
            'version': platform.version(),
            'id': self.machine_id,
        }
        info['root'] = computer_info
        xml_str = xmltodict.unparse(info, pretty=True)
        with open(file + f'/{platform.node()}.nee', 'w') as f:
            f.write(xml_str)
        return f'{platform.node()}.nee'

    @staticmethod
    def check_is_dna(chain_a, chain_b):
        try:
            for i in range(0, len(chain_a)):
                if int(chain_a[i]) + int(chain_b[i]) != 9:
                    return False
            return True
        except:
            return False

    def check_license(self, nee_license=None):
        if self.current_date is None:
            try:
                self.current_date = self.get_current_date()
            except Exception as e:
                raise Exception('network error')
        if (self.encrypts_dict is (None or {}) or self.start_site_list is (None or []) or
                self.intervening_sequence_list is (None or []) or self.end_site_list is (None or [])):
            self.get_encrypts_dict()
        encrypts_machine_id, encrypts_date = self.find_encrypts_data(nee_license)
        machine_id, date = self.decrypts_machine_id(encrypts_machine_id), self.decrypts_date(encrypts_date)
        if machine_id == self.machine_id and datetime.strptime(str(date), "%Y%m%d").date() >= self.current_date:
            return True, date
        else:
            return False, date

    def find_encrypts_data(self, chain_a):
        chain_a, chain_b = self.regenerate_dna_chain(chain_a=chain_a)
        chain_a_list = [chain_a[i:i + 2] for i in range(0, len(chain_a), 2)][2:]
        chain_b_list = [chain_b[i:i + 2] for i in range(0, len(chain_b), 2)][2:]
        start_position_machine_id_list, end_position_machine_id_list = [], []
        start_position_date_list, end_position_date_list = [], []
        if int(chain_a[:2]) > 50:
            encrypts_machine_id_chain = chain_b
            for start_site in self.start_site_list:
                start_position_machine_id_list = start_position_machine_id_list + [i for i, x in enumerate(chain_b_list) if x == start_site]
            for end_site in self.end_site_list:
                end_position_machine_id_list = end_position_machine_id_list + [i for i, x in enumerate(chain_b_list) if x == end_site]
        else:
            encrypts_machine_id_chain = chain_a
            for start_site in self.start_site_list:
                start_position_machine_id_list = start_position_machine_id_list + [i for i, x in enumerate(chain_a_list) if x == start_site]
            for end_site in self.end_site_list:
                end_position_machine_id_list = end_position_machine_id_list + [i for i, x in enumerate(chain_a_list) if x == end_site]
        start_position_machine_id, end_position_machine_id = min(start_position_machine_id_list), min(end_position_machine_id_list)
        encrypts_machine_id = encrypts_machine_id_chain[(start_position_machine_id + 3) * 2:(end_position_machine_id + 2) * 2]
        if int(chain_a[2:4]) > 50:
            encrypts_date_chain = chain_b
            for start_site in self.start_site_list:
                start_position_date_list = start_position_date_list + [i for i, x in enumerate(chain_b_list) if x == start_site]
            for end_site in self.end_site_list:
                end_position_date_list = end_position_date_list + [i for i, x in enumerate(chain_b_list) if x == end_site]
        else:
            encrypts_date_chain = chain_a
            for start_site in self.start_site_list:
                start_position_date_list = start_position_date_list + [i for i, x in enumerate(chain_a_list) if x == start_site]
            for end_site in self.end_site_list:
                end_position_date_list = end_position_date_list + [i for i, x in enumerate(chain_a_list) if x == end_site]
        start_position_date, end_position_date = max(start_position_date_list), max(end_position_date_list)
        encrypts_date = encrypts_date_chain[(start_position_date + 3) * 2:(end_position_date + 2) * 2]
        return encrypts_machine_id, encrypts_date

    @staticmethod
    def regenerate_dna_chain(chain_a=None, chain_b=None):
        if chain_a is not None:
            chain_b = ''
            for element_a in chain_a:
                element_b = 9 - int(element_a)
                chain_b = chain_b + str(element_b)
        elif chain_b is not None:
            chain_a = ''
            for element_b in chain_b:
                element_a = 9 - int(element_b)
                chain_a = chain_a + str(element_a)
        return chain_a, chain_b

    def decrypts_dict_name(self, encrypts_dict_name):
        if int(encrypts_dict_name) < 34:
            decrypts_dict_name = 'too'
        elif 67 > int(encrypts_dict_name) >= 34:
            decrypts_dict_name = 'yme'
        else:
            decrypts_dict_name = 'anl'
        return decrypts_dict_name

    def decrypts_date(self, date):
        if self.server_mode:
            encrypts_dict = self.encrypts_dict
        else:
            encrypts_dict_name = date[:2]
            decrypts_dict_name = self.decrypts_dict_name(encrypts_dict_name)
            encrypts_dict = self.offline_encrypts_dict[decrypts_dict_name]
        decrypts_date = ''
        encrypts_date_without_intervening_sequence = ''
        encrypts_date = date[2:]
        for w in [encrypts_date[i:i + 2] for i in range(0, len(encrypts_date), 2)]:
            if w not in self.intervening_sequence_list:
                encrypts_date_without_intervening_sequence = encrypts_date_without_intervening_sequence + w
        for w in [encrypts_date_without_intervening_sequence[i:i + 2] for i in range(0, len(encrypts_date_without_intervening_sequence), 2)]:
            w = encrypts_dict[w]
            decrypts_date = decrypts_date + str(w)
        return decrypts_date

    def decrypts_machine_id(self, encrypts_machine_id):
        if self.server_mode:
            encrypts_dict = self.encrypts_dict
        else:
            encrypts_dict_name = encrypts_machine_id[:2]
            encrypts_dict = self.decrypts_dict_name(encrypts_dict_name)
            encrypts_dict = self.offline_encrypts_dict[encrypts_dict]
        decrypts_machine_id = ''
        machine_id_without_intervening_sequence = ''
        machine_id = encrypts_machine_id[2:]
        for w in [machine_id[i:i + 2] for i in range(0, len(machine_id), 2)]:
            if w not in self.intervening_sequence_list:
                machine_id_without_intervening_sequence = machine_id_without_intervening_sequence + w
        for w in [machine_id_without_intervening_sequence[i:i + 2] for i in range(0, len(machine_id_without_intervening_sequence), 2)]:
            w = encrypts_dict[w]
            decrypts_machine_id = decrypts_machine_id + str(w)
        return decrypts_machine_id

    @staticmethod
    def get_machine_id():
        # 获取硬盘序列号
        try:
            if platform.system() == "Windows":
                # 使用WMIC命令获取硬盘序列号
                result = subprocess.check_output(['wmic', 'diskdrive', 'get', 'serialnumber'],
                                                 creationflags=subprocess.CREATE_NO_WINDOW).decode().strip().split('\n')[1].strip()
            elif platform.system() == "Linux":
                # 使用lsblk命令获取硬盘序列号
                result = subprocess.check_output(['lsblk', '-ndo', 'SERIAL'],
                                                 creationflags=subprocess.CREATE_NO_WINDOW).decode().strip().split('\n')[0]
            elif platform.system() == "Darwin":  # macOS
                # 使用diskutil命令获取硬盘序列号
                result = subprocess.check_output(['diskutil', 'info', '/dev/disk0'],
                                                 creationflags=subprocess.CREATE_NO_WINDOW).decode().strip()
                for line in result.split('\n'):
                    if 'Device / Media Serial Number:' in line:
                        result = line.split(':')[-1].strip()
                        break
            else:
                raise NotImplementedError("Unsupported OS")
            disk_id = result
        except:
            disk_id = None
        # 获取CPU信息
        cpu_info = platform.processor()
        # 生成唯一的机器ID
        unique_string = cpu_info + (str(disk_id) if disk_id else "")
        machine_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string)).replace('-', '')
        return machine_id

    def get_current_date(self):
        while self.current_date is None:
            try:
                response = self.client.request('ntp.aliyun.com', version=3, timeout=15)
                current_date = datetime.fromtimestamp(response.tx_time).date()
            except:
                print(traceback.print_exc())
                self.current_date_try_times += 1
                current_date = None
                if self.current_date_try_times > 3:
                    self.current_date_try_times = 0
                    raise Exception('network error,can\'t get current date')
            finally:
                self.current_date = current_date
            time.sleep(0.1)
        return current_date


if __name__ == '__main__':
    a = License('http://1.12.228.148:42689')
    # a = License()
    boo, date = a.check_license(nee_license='46266786005595761786653334346525172825801817953634375980121469001295696512907757818757515577666884289173372343521572851534122885149757121948866951260833578458016652374000582371523028230118373066256119')
    en_machine_id, en_date = a.find_encrypts_data('12764241597036101127763057138346270985296759003521279588761953475585472809626447815518811661705664477816137716789103537147035509163979065460657731711244864637964417013081810636881605338563188169115303')
    print(en_machine_id, en_date)
    print('8876195347558547280962644781551881166170566447781613771678910353714703', '393422682887551353620355')
    print(boo, date)
    # boo, date = a.check_license(nee_license='96069889982001447091435114431004049794996157187883510915909731622981291750811517430127850633719436038396336619224594616465534261714272649205803821367812164482269824491669031492772167268950350322529570')
