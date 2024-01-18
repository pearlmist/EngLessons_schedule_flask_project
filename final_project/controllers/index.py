from app import app
from flask import render_template, request, session
from datetime import datetime
# import sqlite3
from models.index_model import get_teachers, get_students, get_time_by_date_and_teacher, add_student_to_lesson, get_teacher_by_date_and_time, get_lessons_ultra, get_price, get_lessons_pro_for_teachers
from utils import get_db_connection
# from models.index_model import get_reader, get_book_reader, get_new_reader, return_book,borrow_book


@app.route('/', methods=['GET', 'POST'])
def index():
    conn = get_db_connection()

    # if request.method == 'POST':

    sel_time = request.values.get("selected_time")
    sel_teacher = request.values.get("selected_teacher")
    # print("тип:")
    # print(session['type'])
    if request.values.get("selected_type"):
        session['type'] = request.values.get("selected_type")
        # if type == "day":
        #     df_lessons = get_lessons_report(conn, start_date, count_days, session['level_name'])
        # elif type == "teacher":
        #     df_lessons = get_lessons_report_by_teacher(conn, start_date, count_days, session['level_name'])
        current_type = session['type']
        start_date = request.values.get("start_date")
        session['cur_date_in_schedule'] = ""
        session['cur_time_in_schedule'] = ""

    elif request.values.get("show_with_date"):
        start_date_str = request.values.get("start_date")
        start_date = start_date_str
        session['count_days'] = int(request.values.get("count_days"))
        session['current_date'] = start_date
        level_id = request.values.get("selected_level")
        session['level_name'] = level_id
        current_type = request.values.get("current_type")
        session['cur_date_in_schedule'] = ""
        session['cur_time_in_schedule'] = ""

    elif request.values.get("add_user") and session['type'] == "day":
        user_name = request.values.get("user_name")
        phone_number = request.values.get("phone")
        teacher_name = request.values.get("checkbox_teacher")
        add_student_to_lesson(conn, user_name, phone_number, session['cur_date_in_schedule'][:-5],
                              session['cur_time_in_schedule'], teacher_name, session["level_name"])
        current_type = request.values.get("current_type")
        start_date = request.values.get("start_date")
        session['cur_date_in_schedule'] = ""
        session['cur_time_in_schedule'] = ""

    elif request.values.get("add_user") and session['type'] == "teacher":
        user_name = request.values.get("user_name")
        phone_number = request.values.get("phone")
        selected_time = request.values.get("checkbox_teacher")
        add_student_to_lesson(conn, user_name, phone_number, session['cur_date_in_schedule'][:-5],
                              selected_time, session['cur_teacher_in_schedule'], session["level_name"])
        current_type = request.values.get("current_type")
        session['type'] = current_type
        start_date = request.values.get("start_date")
        session['cur_date_in_schedule'] = ""
        session['cur_teacher_in_schedule'] = ""

    elif sel_time:
        session['cur_date_in_schedule'] = sel_time[:15]
        session['cur_time_in_schedule'] = sel_time[-5:]

        start_date = request.values.get("start_date")
        level_id = request.values.get("selected_level")
        session['level_name'] = level_id
        current_type = request.values.get("current_type")
        session['type'] = current_type

    elif sel_teacher:
        session['cur_date_in_schedule'] = sel_teacher[:15]
        session['cur_teacher_in_schedule'] = sel_teacher[16:]

        start_date = request.values.get("start_date")
        level_id = request.values.get("selected_level")
        session['level_name'] = level_id
        current_type = request.values.get("current_type")
        session['type'] = current_type



    else:
        # df_lessons = get_lessons_report_by_teacher(conn, start_date, count_days, session['level_name'])
        current_type = "day"
        session['type'] = current_type
        session['level_name'] = "1"
        session['current_date'] = datetime.now().date().strftime("%Y-%m-%d")
        start_date = session['current_date']
        session['count_days'] = 5
        session['cur_date_in_schedule'] = ""
        session['cur_time_in_schedule'] = ""



    print(session['current_date'])
    print(session['count_days'])
    print(session['type'])
    print(current_type)

    if current_type == "day":
        # df_lessons = get_lessons_ultra(conn, start_date, count_days, "A")
        # print(start_date, count_days, session['level_name'])
        df_lessons = get_lessons_ultra(conn, start_date, session['count_days'], session['level_name'])
    elif current_type == "teacher":
        df_lessons = get_lessons_pro_for_teachers(conn, start_date, session['count_days'], session['level_name'])
    else:
        df_lessons = None
        print("ошибка")

    if sel_time:
        selected_td = get_teacher_by_date_and_time(conn, sel_time, session['level_name'])
    elif sel_teacher:
        selected_td = get_time_by_date_and_teacher(conn, sel_teacher)
    else:
        selected_td = None

    # print(current_date)

    # date_obj = datetime.strptime(current_date, "%d-%m-%Y")
    # converted_date_str = date_obj.strftime("%Y-%m-%d")
    # print(get_price(conn))
    # print(session['level_name'])
    # print(df_lessons)

    html = render_template(
        'index.html',
        lessons=df_lessons,
        cur_date=session['current_date'],
        cnt_days=session['count_days'],
        lvl_name=int(session['level_name']),
        combo_box=get_price(conn),
        cur_type=current_type,
        get_td=get_teacher_by_date_and_time,
        selected_date=session['cur_date_in_schedule'],
        selected_time=session['cur_time_in_schedule'],
        selected_teacher=session['cur_teacher_in_schedule'],
        sel_td = selected_td,
        len=len
    )
    return html


if __name__ == '__main__':
    app.run(debug=True)