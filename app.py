# Que 1. Develop a python script that connects to MYSQl database and fetch data from multiple tables and merge in main table to generate a consolidated report. 

import pymysql

# <<<---------------------------------------Database connection details--------------------------------------------->>>

host = 'localhost'
user = 'root'
password = ''
database = 'questions'

# <<<-------------------------------Establishing a connection to the database------------------------------------------>>>

conn = pymysql.connect(host=host, user=user, password=password, database=database)
print("Database connected")

# <<<------------------------------Handeling all errors and exceptions---------------------------------->>>

try:

# <<<--------------------------------Creating table Age-------------------------------------------->>>

    # with conn.cursor() as cur:
    #     # Create the Age table
    #     create_table_query = '''
    #         CREATE TABLE Age (
    #             a_id INT PRIMARY KEY,
    #             a_age INT NOT NULL
    #         )
    #     '''
    #     cur.execute(create_table_query)
    #     conn.commit()
    #     print("Table 'Age' created successfully!")

# <<<-------------------------------------Creating table Name------------------------------------------->>>

    # with conn.cursor() as cur:
    #     # Create the Name table
    #     create_table_query = '''
    #         CREATE TABLE Name (
    #             n_id INT PRIMARY KEY,
    #             n_name VARCHAR(50) NOT NULL
    #         )
    #     '''
    #     cur.execute(create_table_query)
    #     conn.commit()
    #     print("Table 'Name' created successfully!")

# <<<---------------------------------------Creating table City---------------------------------------------->>>

    # with conn.cursor() as cur:
    #     # Create the City table
    #     create_table_query = '''
    #         CREATE TABLE City (
    #             c_id INT PRIMARY KEY,
    #             c_city VARCHAR(50) NOT NULL
    #         )
    #     '''
    #     cur.execute(create_table_query)
    #     conn.commit()
    #     print("Table 'City' created successfully!")

# <<<------------------------------------Creating table Student----------------------------------------->>>

    # with conn.cursor() as cur:
    #     # Create the Student table
    #     create_table_query = '''
    #         CREATE TABLE Student (
    #             ID INT PRIMARY KEY,
    #             Name INT,
    #             Age INT,
    #             City INT,
    #             FOREIGN KEY (Name) REFERENCES Name(n_id),
    #             FOREIGN KEY (Age) REFERENCES Age(a_id),
    #             FOREIGN KEY (City) REFERENCES City(c_id)
    #         )
    #     '''
    #     cur.execute(create_table_query)
    #     conn.commit()
    #     print("Table 'Student' created successfully!")

# <<<-------------------------------Inserting data into Age, Name, City tables---------------------------------------------->>>

    # # Insert data into the individual tables
    # with conn.cursor() as cur:
    #     insert_name_query = "INSERT INTO Name (n_id, n_name) VALUES (%s, %s)"
    #     insert_age_query = "INSERT INTO Age (a_id, a_age) VALUES (%s, %s)"
    #     insert_city_query = "INSERT INTO City (c_id, c_city) VALUES (%s, %s)"

    #     cur.execute(insert_name_query, (1, 'John Doe'))
    #     cur.execute(insert_age_query, (1, 25))
    #     cur.execute(insert_city_query, (1, 'New York'))

    #     cur.execute(insert_name_query, (2, 'Jane Smith'))
    #     cur.execute(insert_age_query, (2, 28))
    #     cur.execute(insert_city_query, (2, 'London'))
    #     conn.commit()
    #     print("Data inserted")

# <<<---------------------------------Inserting data into Student table------------------------------------------>>>

    # with conn.cursor() as cur:
    #     insert_student_query = "INSERT INTO Student (ID, Name, Age, City) VALUES (%s, %s, %s, %s)"
    #     cur.execute(insert_student_query, (1, 1, 1, 1))
    #     cur.execute(insert_student_query, (2, 2, 2, 2))
    #     conn.commit()
    #     print("Data inserted into 'Student' table")

# <<<-------------------Fetch data from Student table with inner join on Name, Age, and City tables---------------------------->>>
    
    with conn.cursor() as cur:
        fetch_student_query = '''
            SELECT Student.ID, Name.n_name, Age.a_age, City.c_city
            FROM Student
            INNER JOIN Name 
            ON Student.Name = Name.n_id
            INNER JOIN Age 
            ON Student.Age = Age.a_id
            INNER JOIN City 
            ON Student.City = City.c_id
        '''
        cur.execute(fetch_student_query)
        student_data = cur.fetchall()

# <<<-------------------------------Print the fetched data--------------------------------------------->>>

        student_list = []
        for row in student_data:
            student_dict = {
                'ID': row[0],
                'Name': row[1],
                'Age': row[2],
                'City': row[3]
            }
            student_list.append(student_dict)

#<<<-------------------------------------- Print the fetched data------------------------------------->>>

        print("Student Data:")
        for student in student_list:
            print(student)
        
        print("Data fetched from Student table successfully!")

finally:
    # <<<-----------------------------Closing the database connection----------------------------------------->>>
    conn.close()

