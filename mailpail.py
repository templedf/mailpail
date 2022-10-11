#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 09:14:39 2022

@author: dtempleton
"""

 # Copyright 2022 Daniel Templeton

 #   Licensed under the Apache License, Version 2.0 (the "License");
 #   you may not use this file except in compliance with the License.
 #   You may obtain a copy of the License at

 #       http://www.apache.org/licenses/LICENSE-2.0

 #   Unless required by applicable law or agreed to in writing, software
 #   distributed under the License is distributed on an "AS IS" BASIS,
 #   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 #   See the License for the specific language governing permissions and
 #   limitations under the License.

# Script to deduplicate personal information by address to create
# customized mailing labels.  Also joins customer data with
# historical data for voting counts.

import re
import pandas as pd
import numpy as np

def join_names(names):
    out = ''
    title = ''
    count = 0
    
    for n in names:
        ns = [p for p in n.split(' ') if len(p) > 0]
        # Assume the last part of the name is the last name
        t = ns[-1]

        if count == 0 and len(ns) > 1:
            # If it's the first one, just roll with it
            out = n
            title = t
            count = 1
        elif count == 0:
            # If first name is missing, just call them a family
            title = t
            out = title + ' family'
            count = 3
        else:
            if title == t:
                if count == 1 and len(ns) > 1:
                    # If it's the same last name, prepend with an andpersand
                    out = ' '.join(ns[0:-1]) + ' & ' + out
                    count = 2
                elif count >= 1:
                    # If the last name's missing or if there's more than 3, they're a family
                    out = title + ' family'
                    count = 3
            else:
                # If last names don't match, they're just voters
                out = 'Palo Alto voters'
                count = 3
                break
    
    return out

# Read the TSV
cust_col_types = {'lVoterUniqueID': np.int64, 'sAffNumber': str, 'szStateVoterID': str,
                  'sVoterTitle': str, 'szNameLast': str, 'szNameFirst': str, 'szNameMiddle': str,
                  'sNameSuffix': str, 'sGender': str, 'szSitusAddress': str, 'szSitusCity': str,
                  'sSitusState': str, 'sSitusZip': str, 'sHouseNum': str, 'sUnitAbbr': str,
                  'sUnitNum': str, 'szStreetName': str, 'sStreetSuffix': str, 'sPreDir': str,
                  'sPostDir': str, 'szMailAddress1': str, 'szMailAddress2': str, 'szMailAddress3': str,
                  'szMailAddress4': str, 'szMailZip': str, 'szPhone': str, 'szEmailAddress': str,
                  'dtBirthDate': str, 'sBirthPlace': str, 'dtRegDate': str, 'dtOrigRegDate': str,
                  'dtLastUpdate_dt': str, 'sStatusCode': str, 'szStatusReasonDesc': str, 'sUserCode1': str,
                  'sUserCode2': str, 'iDuplicateIDFlag': str, 'szLanguageName': str, 'szPartyName': str,
                  'szAVStatusAbbr': str, 'szAVStatusDesc': str, 'szPrecinctName': str, 'sPrecinctID': str,
                  'sPrecinctPortion': str, 'sDistrictID_0': str, 'iSubDistrict_0': str, 'szDistrictName_0': str,
                  'sDistrictID_1': str, 'iSubDistrict_1': str, 'szDistrictName_1': str, 'sDistrictID_2': str,
                  'iSubDistrict_2': str, 'szDistrictName_2': str, 'sDistrictID_3': str, 'iSubDistrict_3': str,
                  'szDistrictName_3': str, 'sDistrictID_4': str, 'iSubDistrict_4': str, 'szDistrictName_4': str,
                  'sDistrictID_5': str, 'iSubDistrict_5': str, 'szDistrictName_5': str}
cust = pd.read_csv('cust.tsv', sep='\t', dtype=cust_col_types).set_index('lVoterUniqueID', drop=False)
# Remove OS-MIL, OS-PERM, and OS-TEMP
cust = cust[~cust.szPartyName.str.contains(r'OS-(TEMP|PERM|MIL)')]
# Find the US addresses that are formatted like international addresses
cust_us = cust[cust.szMailAddress4.isnull()][~cust.szMailAddress3.isnull()]
cust_us = cust_us[cust_us.szMailAddress3.str.contains(' ')]
cust_us2 = cust_us[~cust_us.szMailAddress4.isnull()]
cust_us2 = cust_us2[cust_us2.szMailAddress4.str.contains(r'^\d+$')]
# Make the data the US-formatted + the US international-formatted
cust = pd.concat([cust[cust.szMailAddress4.isnull()][cust.szMailAddress3.isnull()], cust_us, cust_us2])
# Combine the relevant mailing addresses into a single column
cust['targetMail'] = cust[['szMailAddress1', 'szMailAddress2']].apply(" ".join, axis=1)
normal = cust[cust.szMailAddress3.isnull()]
weird = cust[~cust.szMailAddress3.isnull()]
weird['strMail3'] = weird['szMailAddress3'].apply(str)
weird['targetMail'] = weird[['targetMail', "strMail3"]].apply(" ".join, axis=1)
cust = pd.concat([normal, weird])
# Remove the "NaN" null names.
cust.loc[cust.szNameFirst.isnull(),'szNameFirst'] = ''
cust.loc[cust.szNameLast.isnull(),'szNameLast'] = ''
# Merge the names columns into a full name column
cust['fullName'] = cust[['szNameFirst', 'szNameLast']].apply(" ".join, axis=1)
# Combine the fullNames column into a list grouped by address
names = cust.groupby('targetMail').agg({'fullName': lambda n: join_names(n)})
data = cust.set_index('targetMail', drop=False).join(names, rsuffix='List').set_index('lVoterUniqueID', drop=False)

# Read the TSV
hist_col_types = {'lVoterUniqueID': np.int64, 'sElectionAbbr': str, 'szElectionDesc': str, 'dtElectionDate': str,
                   'sElecTypeDesc': str, 'sVotingPrecinct': str, 'szVotingMethod': str, 'sPartyAbbr': str,
                   'szPartyName': str, 'szCountedFlag': str, 'szEVSiteDesc': str}
hist = pd.read_csv('hist.tsv', sep='\t', dtype=hist_col_types).set_index('lVoterUniqueID')
# Count the votes and merge them into the data
counts = hist.groupby('lVoterUniqueID').size().rename('vote')
data = data.set_index('lVoterUniqueID', drop=False).join(counts, how='left', rsuffix='Count')
# Combine the counts across households
fullCounts = data[['targetMail', 'vote']].groupby('targetMail').sum()
data = data.set_index('targetMail', drop=False).join(fullCounts, rsuffix='Count').set_index('lVoterUniqueID')

# Reduce the data to the names and addresses and drop duplicates.
# Keep the original mail addresses and the Situs city for manual fixing
# TODO: roll up the party in some useful way
data = data[['fullNameList', 'targetMail', 'szPartyName', 'szMailAddress1', 'szMailAddress2', 'szMailAddress3', 'szSitusCity', 'voteCount']]
data = data.drop_duplicates(subset=['targetMail'])
# Get a count of the residents at each address
residents = cust.groupby('targetMail').size().rename('num_residents')
data = data.set_index('targetMail', drop=False).join(residents)

# Pull the correct state and zip out of the address.
# TODO: Should probably filter out bad states for safety.
data['printZip'] = data['targetMail'].str.extract(r'(\d{5})[-\s0-9]*$')
data['printState'] = data['targetMail'].str.extract(r'\s+([a-zA-Z]{2})\s+\d{5}[-\s0-9]*$')
# To get the city and address, we dave to split up the data into the two format groups
normal = data[data.szMailAddress3.isnull()]
normal['printCity'] = normal['szMailAddress2'].str.extract(r'([-. a-zA-Z]+)\s+[a-zA-Z]{2}\s+\d{5}[-\s0-9]*$')
normal['printAddress'] = normal['szMailAddress1']
weird = data[~data.szMailAddress3.isnull()]
weird['printCity'] = weird['szMailAddress3'].str.extract(r'([-. a-zA-Z]+)\s+[a-zA-Z]{2}\s+\d{5}[-\s0-9]*$')
weird['printAddress'] = weird[['szMailAddress1', 'szMailAddress2']].apply(" ".join, axis=1)
# Put the data back together and print it
data = pd.concat([normal, weird])
data.to_csv('labels.tsv', sep="\t")
