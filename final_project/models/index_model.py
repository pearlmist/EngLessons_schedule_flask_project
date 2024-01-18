import pandas as pd


def get_teachers(conn):
    return pd.read_sql(
        '''SELECT * FROM teacher JOIN price USING (level_id)''',
        conn)


def get_students(conn):
    return pd.read_sql(
        '''SELECT * FROM student''',
        conn)


def get_timetable(conn):
    return pd.read_sql(
        '''SELECT * FROM timetable''',
        conn)


def get_timetable_date(conn):
    return pd.read_sql(
        '''SELECT * FROM timetable_date''',
        conn)


def get_lessons(conn):
    return pd.read_sql(
        '''SELECT * FROM lesson''',
        conn)


def get_price(conn):
    return pd.read_sql(
        '''SELECT * FROM price''',
        conn)


def weekday(full_name):
    short_name = full_name.lower()
    short = (
        'пн' if short_name == 'mon' else
        'вт' if short_name == 'tue' else
        'ср' if short_name == 'wed' else
        'чт' if short_name == 'thu' else
        'пт' if short_name == 'fri' else
        'сб' if short_name == 'sat' else
        'вс'
    )
    return short


def get_weekday(date_str):
    date = pd.to_datetime(date_str, format='%Y-%m-%d')
    eng_weekday = date.strftime('%a').lower()
    rus_weekday = weekday(eng_weekday)
    return rus_weekday


def level_check(level_id):
    if level_id == "1":
        level = "A"
    elif level_id == "2":
        level = "B"
    elif level_id == "3":
        level = "C"
    else:
        level = level_id
        # print("сломалось")
    # print("level - ", level)
    return level


def get_lessons_ultra(conn, start_day, num_days, level_id):
    temp_days = f'+{num_days} day'
    sql = f'''
    WITH RECURSIVE create_time(cur_time) AS (
                SELECT '09:00'
                UNION ALL
                SELECT strftime('%H:%M', TIME(cur_time, '+1 hour'))
                FROM create_time
                WHERE cur_time < '17:00'
            ),
            time_col(Время) AS (
                SELECT *
                FROM create_time
            ),
            idk AS (
              SELECT timetable_date_id, cur_time as Время, receipt_date, COUNT(DISTINCT teacher_id) as Количество
              FROM create_time,
                timetable
              JOIN timetable_date USING (timetable_id)
              JOIN teacher USING (teacher_id)
              JOIN price USING (level_id)
              WHERE cur_time BETWEEN start_time and TIME(finish_time, '-1 hour')
              AND level_name = :level
              AND receipt_date BETWEEN :current_date AND DATE(:current_date, '{temp_days}')
              GROUP BY cur_time, receipt_date 
              ORDER BY cur_time
            )
            '''
    for i in range(0, num_days):
        current_date = pd.to_datetime(start_day, format='%Y-%m-%d') + pd.DateOffset(days=i)
        form_day = current_date.strftime('%Y-%m-%d')
        sql += f''',
            get_day_{i} AS (
              SELECT Время, Количество as "{form_day + f" ({get_weekday(form_day)})"}"
              FROM idk
              WHERE receipt_date = "{form_day}"
              AND Количество IS NOT NULL
            )
      '''

    sql += f'''
    SELECT Время
    '''
    for i in range(0, num_days):
        current_date = pd.to_datetime(start_day, format='%Y-%m-%d') + pd.DateOffset(days=i)
        form_day = current_date.strftime('%Y-%m-%d')
        sql += f''',
            COALESCE("{form_day + f" ({get_weekday(form_day)})"}", 0) as "{form_day + f" ({get_weekday(form_day)})"}"
      '''

    sql += f'''
    FROM time_col
    '''
    for i in range(0, num_days):
        sql += f'''
      LEFT JOIN get_day_{i} USING (Время)
      '''

    df = pd.read_sql(sql, conn,
                     params={'level': level_check(level_id), 'current_date': start_day, 'count_days': temp_days})
    return df


def get_teacher_by_date_and_time(conn, value, level_id):
    date = value[:10]
    time = value[-5:] + ":00"
    # print(date)
    # print(time)
    df = pd.read_sql('''
        SELECT teacher_name as Преподаватели
        FROM lesson
        JOIN timetable_date USING (timetable_date_id)
        JOIN timetable USING (timetable_id)
        JOIN teacher USING (teacher_id)
        JOIN price USING (level_id)
        WHERE lesson_time = :time
        AND receipt_date = :date
        AND student_id is Null
        AND level_name = :level
    ''', conn, params={'time': time, 'date': date, 'level': level_check(level_id)})
    print(df)
    return df


def get_time_by_date_and_teacher(conn, value):
    date = value[:10]
    teacher = value[16:]
    df = pd.read_sql('''
        SELECT lesson_time as Время
        FROM lesson
        JOIN timetable_date USING (timetable_date_id)
        JOIN timetable USING (timetable_id)
        JOIN teacher USING (teacher_id)
        JOIN price USING (level_id)
        WHERE teacher_name = :teacher
        AND receipt_date = :date
        AND student_id is Null
    ''', conn, params={'teacher': teacher, 'date': date})
    return df






# def get_lessons_pro(conn, start_day, num_days, level_id):
#     temp_days = f'+{num_days} day'
#     df = pd.read_sql('''
#     WITH RECURSIVE create_time(cur_time) AS (
#                 SELECT '09:00'
#                 UNION ALL
#                 SELECT strftime('%H:%M', TIME(cur_time, '+1 hour'))
#                 FROM create_time
#                 WHERE cur_time < '18:00'
#             ),
#             idk AS (
#               SELECT timetable_date_id, cur_time, receipt_date, COUNT(DISTINCT teacher_id) as Количество
#               FROM create_time,
#                 timetable
#               JOIN timetable_date USING (timetable_id)
#               JOIN teacher USING (teacher_id)
#               JOIN price USING (level_id)
#               WHERE cur_time BETWEEN start_time and TIME(finish_time, '-1 hour')
#               AND level_name = :level
#               AND receipt_date BETWEEN :current_date AND DATE(:current_date, '+3 day')
#               GROUP BY cur_time, receipt_date
#               ORDER BY cur_time
#             ),
#             get_some_value AS (
#                 SELECT Количество
#                 FROM idk
#                 WHERE receipt_date = :new_date
#                 AND cur_time = :new_time
#             )
#
#     SELECT *
#     FROM get_some_value
#     ''', conn, params={'level': level_check(level_id), 'current_date': start_day, 'count_days': temp_days,
#                        'new_date': current_d, 'new_time': current_t})
#     return df


# def get_lessons_pro_max(conn, start_day, num_days, level_id, current_d=None, current_t=None):
#     temp_days = f'+{num_days} day'
#     sql = '''
#     WITH RECURSIVE create_time(cur_time) AS (
#                 SELECT '09:00'
#                 UNION ALL
#                 SELECT strftime('%H:%M', TIME(cur_time, '+1 hour'))
#                 FROM create_time
#                 WHERE cur_time < '18:00'
#             ),
#             idk AS (
#               SELECT timetable_date_id, cur_time, receipt_date, COUNT(DISTINCT teacher_id) as Количество
#               FROM create_time,
#                 timetable
#               JOIN timetable_date USING (timetable_id)
#               JOIN teacher USING (teacher_id)
#               JOIN price USING (level_id)
#               WHERE cur_time BETWEEN start_time and TIME(finish_time, '-1 hour')
#               AND level_name = :level
#               AND receipt_date BETWEEN :current_date AND DATE(:current_date, '+3 day')
#               GROUP BY cur_time, receipt_date
#               ORDER BY cur_time
#             ),
#             get_some_value AS (
#                 SELECT Количество
#                 FROM idk'''
#     if current_d and current_t:
#         sql += f'''
#                 WHERE cur_time = "{current_t}"
#                 AND receipt_date = "{current_d}"
#         '''
#     sql += '''
#             )
#
#     SELECT *
#     FROM get_some_value
#     '''
#     df = pd.read_sql(sql, conn, params={'level': 'A', 'current_date': start_day, 'count_days': temp_days})
#     return df


# def get_lessons_report(conn, start_date, num_days, level_id):
#     query = f'''
#         WITH RECURSIVE create_time(cur_time) AS (
#             SELECT '09:00:00'
#             UNION ALL
#             SELECT TIME(cur_time, '+1 hour')
#             FROM create_time
#             WHERE cur_time < '18:00:00'
#         ),
#         time_ranges AS (
#             SELECT
#                 strftime('%H:%M', cur_time) || '-' || strftime('%H:%M', TIME(cur_time, '+1 hour')) AS time_range,
#                 strftime('%Y-%m-%d', timetable_date.receipt_date) AS formatted_date
#             FROM timetable
#             JOIN timetable_date ON timetable.timetable_id = timetable_date.timetable_id
#             CROSS JOIN create_time
#         '''
#
#     if level_id:
#         if level_id == "1":
#             level = "A"
#         elif level_id == "2":
#             level = "B"
#         elif level_id == "3":
#             level = "C"
#         else:
#             print("сломалось")
#         print("level - ", level)
#         query += f'''
#             JOIN teacher USING (teacher_id)
#             JOIN price USING (level_id)
#             WHERE level_name = "{level}"
#         '''
#     query += f''')
#         SELECT
#             time_range as "Время",
#             CASE
#                 WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) = 1 THEN '1 преподаватель'
#                 WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) > 1 AND COUNT(CASE WHEN formatted_date = ? THEN 1 END) < 5 THEN CAST(COUNT(CASE WHEN formatted_date = ? THEN 1 END) AS TEXT) || ' преподавателя'
#                 WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) >= 5 THEN CAST(COUNT(CASE WHEN formatted_date = ? THEN 1 END) AS TEXT) || ' преподавателей'
#                 ELSE ''
#             END AS "{start_date + " (" + get_weekday(start_date) + ")"}"
#     '''
#
#     # Добавляем дополнительные столбцы для каждой даты
#     for i in range(1, num_days):
#         current_date = pd.to_datetime(start_date, format='%Y-%m-%d') + pd.DateOffset(days=i)
#         formatted_date = current_date.strftime('%Y-%m-%d')
#         query += f''',
#             CASE
#                 WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) = 1 THEN '1 преподаватель'
#                 WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) > 1 AND COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) < 5 THEN CAST(COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) AS TEXT) || ' преподавателя'
#                 WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) >= 5 THEN CAST(COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) AS TEXT) || ' преподавателей'
#                 ELSE ''
#             END AS "{formatted_date + " (" + get_weekday(formatted_date) + ")"}"
#         '''
#
#     query += '''
#         FROM
#             time_ranges
#         GROUP BY
#             time_range
#         ORDER BY
#             time_range;
#     '''
#
#     return pd.read_sql_query(query, conn,
#                              params=[start_date, start_date, start_date, start_date, start_date, start_date])


def get_lessons_pro_for_teachers(conn, start_day, num_days, level_id):
    temp_days = f'+{num_days} day'
    sql = f'''
    WITH RECURSIVE create_time(cur_time) AS (
                SELECT '09:00'
                UNION ALL
                SELECT strftime('%H:%M', TIME(cur_time, '+1 hour'))
                FROM create_time
                WHERE cur_time < '17:00'
            ),
            time_col(Преподаватель) AS (
                SELECT teacher_name
                FROM teacher
                JOIN price USING (level_id)
                WHERE level_name = :level
                ORDER BY teacher_name
            ),
            idk AS (
              SELECT timetable_date_id, count(cur_time) as Количество, receipt_date, teacher_name as Преподаватель
              FROM create_time,
                timetable
              JOIN timetable_date USING (timetable_id)
              JOIN teacher USING (teacher_id)
              JOIN price USING (level_id)
              WHERE cur_time BETWEEN start_time and TIME(finish_time, '-1 hour')
              AND level_name = :level
              AND receipt_date BETWEEN :current_date AND DATE(:current_date, '{temp_days}')
              GROUP BY teacher_id, receipt_date
              ORDER BY teacher_name
            )
            '''
    for i in range(0, num_days):
        current_date = pd.to_datetime(start_day, format='%Y-%m-%d') + pd.DateOffset(days=i)
        form_day = current_date.strftime('%Y-%m-%d')
        sql += f''',
            get_day_{i} AS (
              SELECT Преподаватель, Количество as "{form_day + f" ({get_weekday(form_day)})"}"
              FROM idk
              WHERE receipt_date = "{form_day}"
              AND Количество IS NOT NULL
              ORDER BY Преподаватель
            )
      '''

    sql += f'''
    SELECT Преподаватель
    '''
    for i in range(0, num_days):
        current_date = pd.to_datetime(start_day, format='%Y-%m-%d') + pd.DateOffset(days=i)
        form_day = current_date.strftime('%Y-%m-%d')
        sql += f''',
            COALESCE("{form_day + f" ({get_weekday(form_day)})"}", 0) as "{form_day + f" ({get_weekday(form_day)})"}"
      '''

    sql += f'''
    FROM time_col
    '''
    for i in range(0, num_days):
        sql += f'''
      LEFT JOIN get_day_{i} USING (Преподаватель)
      '''
    sql += '''
    ORDER BY Преподаватель
    '''
    df = pd.read_sql(sql, conn,
                     params={'level': level_check(level_id), 'current_date': start_day, 'count_days': temp_days})
    return df


def add_student(conn, student_name, phone_number):
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO student (student_name, phone_number) 
                      VALUES (?, ?)''', (student_name, phone_number))
    conn.commit()
    df = pd.read_sql('''SELECT COUNT(student_id) FROM student''', conn)
    return int(df.iloc[0, 0])


def find_teacher(conn, teacher_name, level):
    level_temp = level_check(level)
    df = pd.read_sql('''
        SELECT teacher_id
        FROM teacher
        JOIN price USING (level_id)
        WHERE teacher_name = :name
        AND level_name = :level
    ''', conn, params={'name': teacher_name, 'level': level_temp})
    return int(df.iloc[0, 0])


def find_lesson_id(conn, teacher_id, day, time):
    print(teacher_id, day, time)
    if len(time) == 5:
        time_temp = time + ":00"
    else:
        time_temp = time
    df = pd.read_sql('''
      SELECT lesson_id
      FROM lesson
      JOIN timetable_date USING (timetable_date_id)
      JOIN timetable USING (timetable_id)
      WHERE lesson_time = :time
      AND receipt_date = :day
      AND teacher_id = :id
    ''', conn, params={'id': teacher_id, 'day': day, 'time': time_temp})
    return int(df.iloc[0, 0])


def add_student_to_lesson(conn, student_name, phone_number, day, time, teacher_name, level):
    print('тест2:')
    print(student_name, phone_number, day, time, teacher_name, level)
    teacher_id = find_teacher(conn, teacher_name, level)
    print(teacher_id)
    student_id = add_student(conn, student_name, phone_number)
    print(student_id)
    lesson_id = find_lesson_id(conn, teacher_id, day, time)
    print(lesson_id)
    cursor = conn.cursor()
    cursor.execute("UPDATE lesson SET student_id = " + str(student_id) + " WHERE lesson_id = " + str(lesson_id))
    conn.commit()
    return 0


# def get_lessons_report_by_teacher(conn, start_date, num_days, level_id):
#     query = f'''
#             WITH time_ranges AS (
#                 SELECT
#                     teacher_name,
#                     strftime('%Y-%m-%d', timetable_date.receipt_date) AS formatted_date
#                 FROM timetable
#                 JOIN timetable_date ON timetable.timetable_id = timetable_date.timetable_id
#                 JOIN teacher USING (teacher_id)
#         '''
#
#     if level_id:
#         if level_id == "1":
#             level = "A"
#         elif level_id == "2":
#             level = "B"
#         elif level_id == "3":
#             level = "C"
#         else:
#             print("сломалось")
#         print("level - ", level)
#         query += f'''
#                 JOIN price USING (level_id)
#                 WHERE level_name = "{level}"
#             '''
#     query += f''')
#             SELECT
#                 teacher_name as "Преподаватель",
#                 CASE
#                     WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) = 1 THEN '1 занятие'
#                     WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) > 1 AND COUNT(CASE WHEN formatted_date = ? THEN 1 END) < 5 THEN CAST(COUNT(CASE WHEN formatted_date = ? THEN 1 END) AS TEXT) || ' занятия'
#                     WHEN COUNT(CASE WHEN formatted_date = ? THEN 1 END) >= 5 THEN CAST(COUNT(CASE WHEN formatted_date = ? THEN 1 END) AS TEXT) || ' занятий'
#                     ELSE ''
#                 END AS "{start_date + " (" + get_weekday(start_date) + ")"}"
#         '''
#
#     # Добавляем дополнительные столбцы для каждой даты
#     for i in range(1, num_days):
#         current_date = pd.to_datetime(start_date, format='%Y-%m-%d') + pd.DateOffset(days=i)
#         formatted_date = current_date.strftime('%Y-%m-%d')
#         query += f''',
#                 CASE
#                     WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) = 1 THEN '1 занятие'
#                     WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) > 1 AND COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) < 5 THEN CAST(COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) AS TEXT) || ' занятия'
#                     WHEN COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) >= 5 THEN CAST(COUNT(CASE WHEN formatted_date = '{formatted_date}' THEN 1 END) AS TEXT) || ' занятий'
#                     ELSE ''
#                 END AS "{formatted_date + " (" + get_weekday(formatted_date) + ")"}"
#             '''
#
#     query += '''
#             FROM
#                 time_ranges
#             GROUP BY
#                 teacher_name
#             ORDER BY
#                 teacher_name;
#         '''
#
#     return pd.read_sql_query(query, conn,
#                              params=[start_date, start_date, start_date, start_date, start_date, start_date])

#
# def get_book_reader(conn, reader_id):
#     # выбираем и выводим записи  о том, какие книги брал читатель
#     return pandas.read_sql('''
#                            WITH get_authors( book_id, authors_name)
#                            AS(SELECT book_id, GROUP_CONCAT(author_name)
#                            FROM author
#                            JOIN book_author USING(author_id)
#                            GROUP BY book_id)
#
# SELECT title AS Название, authors_name AS Авторы,borrow_date AS Дата_выдачи, return_date AS Дата_возврата,
# book_reader_id FROM reader JOIN book_reader USING(reader_id) JOIN book USING(book_id) JOIN get_authors USING(
# book_id) WHERE reader.reader_id = :id ORDER BY 3''', conn, params={"id": reader_id})
#
#
# def return_book(conn, book_reader_id):
#     cur = conn.cursor()
#     cur.execute('''
#         UPDATE book_reader
#         SET return_date = DATE('now')
#         WHERE book_reader_id = :book_reader_id
#     ''', {'book_reader_id': book_reader_id})
#     conn.commit()
#     cur.execute('''
#         UPDATE book
#         SET available_numbers = available_numbers + 1
#         WHERE book_id = (SELECT book_id FROM book_reader WHERE book_reader_id = :book_reader_id);
#     ''', {'book_reader_id': book_reader_id})
#     conn.commit()
#
#
# # для обработки данных о новом читателе
# def get_new_reader(conn, new_reader):
#     cur = conn.cursor()
#     cur.execute(
#         '''
#             INSERT INTO reader (reader_name)
#             VALUES (:new_reader)
#         ''',
#         {"new_reader": new_reader}
#     )
#     conn.commit()
#     return cur.lastrowid
#
#
# # для обработки данных о взятой книге
# def borrow_book(conn, book_id, reader_id):
#     cur = conn.cursor()
#     # Используйте параметры для передачи значений в запрос
#     cur.execute('''
#         INSERT INTO book_reader (book_id, reader_id, borrow_date)
#         VALUES (?, ?, DATE('now'))
#     ''', (book_id, reader_id))
#     conn.commit()
#
#     # Используйте именованные параметры в запросе
#     cur.execute('''
#         UPDATE book
#         SET available_numbers = available_numbers - 1
#         WHERE book_id = :book_id
#     ''', {'book_id': book_id})
#     conn.commit()
#
#     return True
