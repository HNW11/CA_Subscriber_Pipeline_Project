#!/usr/bin/env python
# coding: utf-8

# # Subscriber Pipeline Project 

# In[66]:


import sqlite3
import ast
import pandas as pd
import numpy as np


# # Connect SQLite3 and Database

# In[2]:


con = sqlite3.connect('cademycode.db')
cur = con.cursor()


# #### View first row of table

# In[4]:


tableone = cur.execute('''SELECT * FROM sqlite_master''').fetchone()
print(tableone)


# #### View table names

# In[6]:


table_names = cur.execute('''SELECT name FROM sqlite_master WHERE type = 'table'; ''').fetchall()
print(table_names)


# #### Use pandas.read_sql_query to read each table in as a DataFrame

# In[9]:


students = pd.read_sql_query('''SELECT * FROM cademycode_students''', con)
courses = pd.read_sql_query('''SELECT * FROM cademycode_courses''', con)
student_jobs = pd.read_sql_query('''SELECT * FROM cademycode_student_jobs''', con)


# # Inspect and Clean Data

# ## Student Table
# 
# The `student` table is the largest one so I want to spend my time there first

# In[24]:


students.head()


# In[19]:


students.info()


# ### Observations
# There are no integers despite there being some columns that would likely have integers or float.<br>
# DOB should be a datetime. Age would likely be a helpful measurment rather than DOB for grouping purposes. 
# 
# #### Missing Data
# __Job ID__: missing 5<br>
# __Number of Courses Taken__: missing 251<br>
# __Current Career Path ID__: missing 471<br>
# __Time Spent Hours__: missing 471

# ### Age 
# Calculating age as an integer would make grouping easier.

# In[29]:


now = pd.to_datetime('now')
dt_dob = pd.to_datetime(students['dob'])
students['age'] = (now - dt_dob).astype('<m8[Y]')


# In[30]:


students.head()


# ## `Contact Info`
# `Contact Info` is a dictionary. I want to explode the keys into columns. 

# In[43]:


students['contact_info'] = students["contact_info"].apply(lambda x: ast.literal_eval(x))
explode_contact = pd.json_normalize(students['contact_info'])
students = pd.concat([students.drop('contact_info', axis=1), explode_contact], axis=1)


# In[44]:


students.head()


# The mailing address column is very croweded and can be split

# In[45]:


split_mailing = students.mailing_address.str.split(',', expand=True)
split_mailing.columns = ['street', 'city', 'state', 'zip']
students = pd.concat([students.drop('mailing_address', axis=1), split_mailing], axis=1)
students.head()


# In[46]:


students.info()


# ## Datatypes
# There are quite a few numerical columns that are objects<br>
# `num_courses_taken` and `time_spent_hrs` should be floats<br>
# Addtionally I am going to covert `job_id` and `current_career_path_id` to a float 

# In[48]:


students['job_id'] = students['job_id'].astype(float)
students['current_career_path_id'] = students['current_career_path_id'].astype(float)
students['num_course_taken'] = students['num_course_taken'].astype(float)
students['time_spent_hrs'] = students['time_spent_hrs'].astype(float)

students.info()


# ## Missing Data
# ### `Job ID`

# In[50]:


missing_job_id = students[students[['job_id']].isnull().any(axis=1)]
missing_job_id.head()


# There is no obvious pattern from these 5 rows which means it might be MAR data. I don't think that deleting the 5 rows will have any impact on the data. 

# In[52]:


missing_data = pd.DataFrame()
missing_data = pd.concat([missing_data, missing_job_id])
students = students.dropna(subset=['job_id'])


# In[53]:


students.info()


# ### `Number Courses Taken`

# In[55]:


missing_num_course_taken = students[students[['num_course_taken']].isnull().any(axis=1)]
missing_num_course_taken.head(10)


# There is no obvious pattern from the first 10 rows of data. Looking for a distribution pattern can tell me if this is MAR or MNAR. Graphs can help find MNAR data. 

# In[59]:


sg = (students.groupby('job_id').count()['uuid']/len(students)).rename('complete')
mg = (missing_num_course_taken.groupby('job_id').count()['uuid']/len(missing_num_course_taken)).rename('incomplete')
df = pd.concat([sg, mg], axis=1)
df.plot.bar()


# In[58]:


sg = (students.groupby('sex').count()['uuid']/len(students)).rename('complete')
mg = (missing_num_course_taken.groupby('sex').count()['uuid']/len(missing_num_course_taken)).rename('incomplete')
df = pd.concat([sg, mg], axis=1)
df.plot.bar()


# There is no obvious pattern from these graphs and there is nothing out of distribution.  Therefore this is MAR data. Even though its more than the job_id MAR data it is still not as large of a chunk of data that would impact the outcome if I delete it. 

# In[60]:


missing_data = pd.concat([missing_data, missing_num_course_taken])
students = students.dropna(subset=['num_course_taken'])


# In[61]:


students.info()


# ### `Current Carrer Path ID`

# In[63]:


missing_career_path_id = students[students[['current_career_path_id']].isnull().any(axis=1)]
missing_career_path_id.head()


# I noticed this on the last info dump but I wanted to see if it translated in the missing dataframe.  current_career_path_id and time_spend_hours are null together.  This would mean that both are structually missing. To resolve this, I will set current_career_path_id to a new id that will indicate no current career path, and I will set time_spent_hrs to 0 to indicate no hours spent.

# In[64]:


students['current_career_path_id'].unique()


# In[67]:


students['current_career_path_id'] = np.where(students['current_career_path_id'].isnull(), 0, students['current_career_path_id'])
students['time_spent_hrs'] = np.where(students['time_spent_hrs'].isnull(), 0, students['time_spent_hrs'])


# In[68]:


students.info()


# Now that I have gotten rid of a null values I can convert the id categories to integers

# In[79]:


students['job_id'] = students['job_id'].astype(int)
students['current_career_path_id'] = students['current_career_path_id'].astype(int)


# In[80]:


students.info()


# ## Courses Table

# In[71]:


display(courses)


# In[72]:


courses.info()


# The only thing that really needs to be done here is to add the new career path that I created in the students table. 

# In[73]:


undecided = {'career_path_id': 0,
                  'career_path_name': 'undecided',
                  'hours_to_complete': 0}
courses.loc[len(courses)] = undecided
display(courses)


# ## Student Jobs Table

# In[74]:


display(student_jobs)


# In[76]:


student_jobs.info()


# The only thing that needs to be fixed in this table is that there are 3 duplicates. 

# In[77]:


student_jobs.drop_duplicates(inplace=True)
display(student_jobs)


# # Create the Output CSV
# Use the cleaned tables to produce an analytics-ready SQLite database and flat CSV file. The final CSV should contain all the data the analysts might need in a single table. <br>
# 
# I want to keep the data in the students table since it is the largest. I will left join to that one. 

# In[82]:


df = students.merge(courses, left_on='current_career_path_id', right_on = 'career_path_id', how='left')
df = df.merge(student_jobs, on = 'job_id', how='left')
df.head()


# In[83]:


df.info()


# In[84]:


con.close()


# In[ ]:




