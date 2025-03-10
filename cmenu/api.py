import frappe
from frappe.utils.password import check_password
from frappe.utils import nowdate, getdate,get_site_path
from frappe.utils import now, format_time, today
from frappe.utils import nowdate, getdate, add_days
from datetime import datetime, timezone, timedelta, time ,date
from frappe.utils.data import money_in_words
import json
from frappe import _, throw
from geopy.distance import geodesic
from pytz import timezone, utc
from frappe.utils import  add_days, time_diff_in_hours
from frappe.utils.xlsxutils import make_xlsx
from frappe.desk.utils import get_csv_bytes, pop_csv_params, provide_binary_file
from frappe.utils.pdf import get_pdf
from frappe.utils.file_manager import save_file
from jinja2 import Template



def schedule_hourly_attendance():
    attendance_settings = frappe.get_single("Attendance Settings")
    back_days = attendance_settings.get("backdays") or 3  # Default to 3 days back
    current_date = frappe.utils.today()
    start_date = frappe.utils.add_days(current_date, -back_days)

    #employees = frappe.get_all("Employee", filters={"status": "Active","branch":"UNIT-1","employee":"BWH00002"}, fields=["name","employee"])

    for i in range(back_days):
        process_date = frappe.utils.add_days(start_date, i)
        process_attendance_for_date(process_date)

def process_attendance_for_date(attendance_date):
    #frappe.log_error(attendance_date, "attendance_date")
    query = """ 
        SELECT 
    e.employee AS employee_code,
    e.employee_name AS staff_name,
    %s AS date,  

    -- Fetch IN punch
    (SELECT MIN(ec.time) 
     FROM `tabEmployee Checkin` ec 
     WHERE ec.employee = e.employee 
       AND (
            (e.branch = 'UNIT-1' AND ec.log_type = 'IN') -- For UNIT-1: Use log_type = 'IN'
            OR 
            (e.branch IN ('UNIT-2', 'UNIT-4','HO')) -- For UNIT-2: Consider all timestamps (no log_type)
        )
       AND (
            (DATE(ec.time) = %s)  -- Regular IN (same day)
            OR 
            (DATE(ec.time) = %s AND TIME(ec.time) >= '18:00:00')  -- Night shift IN after 6 PM
        )
    ) AS punch_1,

    -- Fetch OUT punch
    (SELECT MAX(ec2.time) 
     FROM `tabEmployee Checkin` ec2 
     WHERE ec2.employee = e.employee 
       AND (
            (e.branch = 'UNIT-1' AND ec2.log_type = 'OUT') -- For UNIT-1: Use log_type = 'OUT'
            OR 
            (e.branch IN ('UNIT-2', 'UNIT-4','HO')) -- For UNIT-2: Consider all timestamps (no log_type)
        )
       AND (
            (DATE(ec2.time) = %s AND TIME(ec2.time) > '11:00:00')  -- Regular OUT (same day)
            OR 
            (DATE(ec2.time) = DATE_ADD(%s, INTERVAL 1 DAY) AND TIME(ec2.time) <= '11:00:00')  -- Night shift OUT before 11 AM next day
        )
    ) AS punch_2,

    e.department AS department,
    e.attendance_device_id AS staff_id,
    a.status AS attendance,
    a.leave_type AS leave_type,
    e.holiday_list AS holiday_list,
    
    -- Get the latest shift assignment
    (SELECT s.shift_type 
     FROM `tabShift Assignment` s 
     WHERE s.employee = e.employee 
       AND s.docstatus = 1
       AND s.start_date <= %s 
     ORDER BY s.start_date DESC 
     LIMIT 1) AS shift

FROM 
    `tabEmployee` e
LEFT JOIN 
    `tabAttendance` a ON e.employee = a.employee 
    AND a.attendance_date = %s 
    AND a.docstatus != 2
WHERE 
    e.status = 'Active'
    AND e.date_of_joining <= %s
ORDER BY 
    e.employee;
    """
    
    data = frappe.db.sql(query, (attendance_date, attendance_date, attendance_date, attendance_date,attendance_date,attendance_date,attendance_date,attendance_date), as_dict=True)

    # Process the data to format punch time and attendance
    for entry in data:
        # Format punch times to 12-hour format
        entry['cpunch_1'] = entry['punch_1']
        entry['cpunch_2'] = entry['punch_2']
        entry['punch_1'] = format_time_to_12hr(entry['punch_1'])
        entry['punch_2'] = format_time_to_12hr(entry['punch_2'])

        # If there is no second punch, show it as empty instead of repeating punch_1
        if not entry['punch_2'] or entry['punch_2'] == entry['punch_1']:
            entry['punch_2'] = ""

        # Check attendance status and include leave type if applicable
        if entry['attendance'] == 'On Leave':
            entry['attendance'] = entry.get('leave_type', '')
         # Check if it's a holiday or weekly off
        holiday_status = check_holiday_status(entry['employee_code'], entry['holiday_list'], attendance_date)

        # Calculate working hours
        if entry['punch_1'] and entry['punch_2']:
            entry['hours'] = calculate_hours(entry['cpunch_1'], entry['cpunch_2'])
        else:
            entry['hours'] = 0
        create_or_update_attendance(entry["employee_code"], attendance_date, entry['hours'],holiday_status)

#acquittance REPORT ---------------------------------------
def get_current_fiscal_year():
    today = frappe.utils.today()
    fiscal_year = frappe.db.get_value("Fiscal Year", {"year_start_date": ["<=", today], "year_end_date": [">=", today]}, "name")
    return fiscal_year

@frappe.whitelist()
def download_Internalbank_csv(**kwargs):
    """
    Fetch net salary and employee details with bank filter (NEFT/Other).
    """

    # Extract filters from request
    filters = {key: value for key, value in kwargs.items() if value}

    # Convert `docstatus` from string to integer
    docstatus_map = {"Draft": 0, "Submitted": 1, "Cancelled": 2}
    docstatus_str = filters.get("docstatus", "Submitted")  # Default to "Submitted"
    docstatus = docstatus_map.get(docstatus_str, 1)  # Default to 1 if not found

    # Construct WHERE conditions for SQL
    conditions = f"WHERE ss.docstatus = {docstatus}"

    if filters.get("from_date") and filters.get("to_date"):
        conditions += f" AND ss.start_date >= '{filters.get('from_date')}' AND ss.end_date <= '{filters.get('to_date')}'"

    if filters.get("company"):
        conditions += f" AND ss.company = '{filters.get('company')}'"

    if filters.get("department"):
        conditions += f" AND emp.department = '{filters.get('department')}'"

    if filters.get("designation"):
        conditions += f" AND emp.designation = '{filters.get('designation')}'"

    if filters.get("branch"):
        conditions += f" AND emp.branch = '{filters.get('branch')}'"

    if filters.get("employee"):
        conditions += f" AND emp.name = '{filters.get('employee')}'"

    # Apply bank filter based on NEFT or Other
    conditions += " AND emp.bank_name = 'FBL'"  # Include only FBL

    # SQL Query to fetch only net pay and employee details
    query = f"""
        SELECT
            emp.employee_name,
            emp.bank_ac_no,
            ss.net_pay
        FROM `tabSalary Slip` AS ss
        JOIN `tabEmployee` AS emp ON ss.employee = emp.name
        {conditions};
    """

    salaries = frappe.db.sql(query, as_dict=True)

    bankdetails = frappe.get_doc("Bank Account", "Salary-FBL - FBL")
    fiscal_year = frappe.defaults.get_user_default("fiscal_year")

    if not salaries:
        frappe.response["type"] = "json"
        frappe.response["message"] = {
            "status": "error",
            "title": "No Salary Data Found",
            "description": "No salary records match the selected filters or no employees are mapped to Bank FBL. Please adjust your filters and try again."
        }
        return

    csv_data = []
    fiscal_year = get_current_fiscal_year()
    date_obj = datetime.strptime(filters.get('from_date'), "%Y-%m-%d")
    month_name = date_obj.strftime("%B")
    csv_data.append([f"FEDERAL BANK SALARY LIST MONTH OF {month_name} {fiscal_year}"])
    # Define CSV file headers
    csv_data.append(["Sr No", "Account Number", "Sol Id", "D/C", "Amount", "Remarks", "Name"])

    # Add salary details
    for index, row in enumerate(salaries, start=1):
        csv_data.append([index, row.bank_ac_no, bankdetails.custom_sol_id, "C", row.net_pay, "Salary", row.employee_name])

    # Add Debit row at the end (Total Amount Debit)
    total_amount = sum([row["net_pay"] for row in salaries])
    debit_row = ["", bankdetails.custom_dr_acct, bankdetails.custom_sol_id, "D", total_amount, "SALARY", "BALAVIGNA"]
    csv_data.append(debit_row)

    # Build the response manually
    csv_content = "\n".join([",".join(map(str, row)) for row in csv_data])
    frappe.response.filename = "Salary_Payment_Bank.csv"
    frappe.response.filecontent = csv_content
    frappe.response.type = "download"

    return


@frappe.whitelist()
def download_bank_csv(**kwargs):
    """
    Fetch net salary and employee details with bank filter (NEFT/Other).
    """

    # Extract filters from request
    filters = {key: value for key, value in kwargs.items() if value}

    # Convert `docstatus` from string to integer
    docstatus_map = {"Draft": 0, "Submitted": 1, "Cancelled": 2}
    docstatus_str = filters.get("docstatus", "Submitted")  # Default to "Submitted"
    docstatus = docstatus_map.get(docstatus_str, 1)  # Default to 1 if not found

    # Construct WHERE conditions for SQL
    conditions = f"WHERE ss.docstatus = {docstatus}"

    if filters.get("from_date") and filters.get("to_date"):
        conditions += f" AND ss.start_date >= '{filters.get('from_date')}' AND ss.end_date <= '{filters.get('to_date')}'"

    if filters.get("company"):
        conditions += f" AND ss.company = '{filters.get('company')}'"

    if filters.get("department"):
        conditions += f" AND emp.department = '{filters.get('department')}'"

    if filters.get("designation"):
        conditions += f" AND emp.designation = '{filters.get('designation')}'"

    if filters.get("branch"):
        conditions += f" AND emp.branch = '{filters.get('branch')}'"

    if filters.get("employee"):
        conditions += f" AND emp.name = '{filters.get('employee')}'"

    # Apply bank filter based on NEFT or Other
    bank_type = filters.get("bank_type", "NEFT")
    conditions += " AND emp.bank_name != 'FBL'"  # Exclude FBL

    # SQL Query to fetch only net pay and employee details
    query = f"""
        SELECT
            emp.employee_name,
            emp.bank_ac_no,
            emp.ifsc_code,
            ss.net_pay
        FROM `tabSalary Slip` AS ss
        JOIN `tabEmployee` AS emp ON ss.employee = emp.name
        {conditions};
    """

    salaries = frappe.db.sql(query, as_dict=True)

    bankdetails = frappe.get_doc("Bank Account", "Salary-FBL - FBL")
    fiscal_year = frappe.defaults.get_user_default("fiscal_year")

    if not salaries:
        frappe.response["type"] = "json"
        frappe.response["message"] = {
            "status": "error",
            "title": "No Salary Data Found",
            "description": "No salary records match the selected filters . Please adjust your filters and try again."
        }
        return

    csv_data = []
    fiscal_year = get_current_fiscal_year()
    date_obj = datetime.strptime(filters.get('from_date'), "%Y-%m-%d")
    month_name = date_obj.strftime("%B")
    #csv_data.append([f"FEDERAL BANK SALARY LIST MONTH OF {month_name} {fiscal_year}"])
    # Define CSV file headers
    csv_data.append(["Sr No", "Dr Acct", "Amount(With Decimals)", "Beneficiary IFSC", "Tran Particular", "BenefCust AcctID", "Benef Cust Name","Benf Cust Addr1","Ordering Bank Code","Ordering Br Code","Paymnt Detail1 (email)","Sender Receiver Info1","Charge Acct"])

    # Add salary details
    for index, row in enumerate(salaries, start=1):
        csv_data.append([index, bankdetails.custom_dr_acct,row.net_pay,row.ifsc_code,"SALARY", row.bank_ac_no,row.employee_name,"DINDIGUL", bankdetails.custom_ordering_bank_code,bankdetails.custom_ordering_br_code, bankdetails.custom_paymnt_detail1_email, bankdetails.custom_sender_receiver_info1, bankdetails.custom_charge_acct ])

    # Add Debit row at the end (Total Amount Debit)
    total_amount = sum([row["net_pay"] for row in salaries])
    debit_row = ["", "Total Amount", total_amount]
    csv_data.append(debit_row)
    debit_row = ["", "", ""]
    csv_data.append(debit_row)
    amount_in_words = money_in_words(total_amount, "INR")
    debit_row = ["", "Amount In Words :-", amount_in_words]
    csv_data.append(debit_row)

    # Build the response manually
    csv_content = "\n".join([",".join(map(str, row)) for row in csv_data])
    frappe.response.filename = "Salary_Payment_Bank.csv"
    frappe.response.filecontent = csv_content
    frappe.response.type = "download"

    return



@frappe.whitelist()
def salary_acquittance_report(**filters):
    """
    Generates a Salary Acquittance Report as a PDF using an SQL Query.
    """

    # Convert `docstatus` from string to integer
    docstatus_map = {"Draft": 0, "Submitted": 1, "Cancelled": 2}
    docstatus_str = filters.get("docstatus", "Submitted")  # Default to "Submitted"
    docstatus = docstatus_map.get(docstatus_str, 1)  # Default to 1 if not found

    # Construct WHERE conditions for SQL
    conditions = f"WHERE ss.docstatus = {docstatus}"
    
    if filters.get("from_date") and filters.get("to_date"):
        conditions += f" AND ss.start_date >= '{filters.get('from_date')}' AND ss.end_date <= '{filters.get('to_date')}'"
    
    if filters.get("company"):
        conditions += f" AND ss.company = '{filters.get('company')}'"

    # SQL Query to fetch salary details with earnings & deductions
    query = f"""
        SELECT
    emp.employee_name,
    emp.designation,
    ss.total_working_days,
    CAST(ss.net_pay AS SIGNED) AS net_pay,
    COALESCE(CAST(earnings.basic AS SIGNED), 0) AS basic,
    COALESCE(CAST(earnings.hra AS SIGNED), 0) AS hra,
    COALESCE(CAST(deductions.pf AS SIGNED), 0) AS pf,
    COALESCE(CAST(deductions.esi AS SIGNED), 0) AS esi,
    CAST(ss.gross_pay AS SIGNED) AS gross_pay,
    COALESCE(CAST(deductions.total_deductions AS SIGNED), 0) AS total_deductions
FROM `tabSalary Slip` AS ss
JOIN `tabEmployee` AS emp ON ss.employee = emp.name

-- Get earnings (Basic, HRA)
LEFT JOIN (
    SELECT
        parent, 
        MAX(CASE WHEN salary_component = 'Basic' THEN CAST(amount AS SIGNED) END) AS basic,
        MAX(CASE WHEN salary_component = 'House Rent Allowance' THEN CAST(amount AS SIGNED) END) AS hra
    FROM `tabSalary Detail`
    WHERE parentfield = 'earnings'
    GROUP BY parent
) AS earnings ON ss.name = earnings.parent

-- Get deductions (PF, ESI, Total Deductions)
LEFT JOIN (
    SELECT
        parent, 
        MAX(CASE WHEN salary_component = 'Provident Fund' THEN CAST(amount AS SIGNED) END) AS pf,
        MAX(CASE WHEN salary_component = 'Employee State Insurance' THEN CAST(amount AS SIGNED) END) AS esi,
        SUM(amount) AS total_deductions
    FROM `tabSalary Detail`
    WHERE parentfield = 'deductions'
    GROUP BY parent
) AS deductions ON ss.name = deductions.parent

{conditions};

    """

    salary_data = frappe.db.sql(query, as_dict=True)

    # Render Jinja template
    html_content = frappe.render_template(
        "cmenu/templates/salary_acquittance_report.html", 
        {"data": salary_data, "filters": filters}
    )

    # Convert to PDF
    pdf_content = get_pdf(html_content)

    # Serve PDF directly (No need to save)
    frappe.local.response.filename = "Salary_Acquittance.pdf"
    frappe.local.response.filecontent = pdf_content
    frappe.local.response.type = "pdf"

#acquittance REPORT ---------------------------------------



def check_holiday_status(employee_id, holiday_list, attendance_date):
    """Check if the given date is a holiday or a weekly off."""
    if not holiday_list:
        return None  # No holiday list assigned

    is_sunday = frappe.utils.getdate(attendance_date).weekday() == 6  # Sunday is weekday 6

    is_holiday = frappe.db.exists("Holiday", {
        "parent": holiday_list,
        "holiday_date": attendance_date
    })

    if is_sunday:
        return "Week Off"
    elif is_holiday:
        return "Holiday"
    
    return None  # Not a holiday or a weekly off

def format_time_to_12hr(punch_time):
    if punch_time:
        return frappe.utils.format_datetime(punch_time, "h:mm a")
    return ""

def format_duration(seconds):
    """Convert seconds to HH:MM:SS format."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

def ctime_diff_in_seconds(end_time, start_time):
    """Calculate time difference in seconds, adjusting for night shift cases."""
    # fmt = "%Y-%m-%d %H:%M:%S"
    
    # start = datetime.strptime(start_time, fmt)
    # end = datetime.strptime(end_time, fmt)

    # If end time is earlier than start time, add one day (handles night shift)
    if end_time < start_time:
        end_time += timedelta(days=1)

    return (end_time - start_time).total_seconds()

def calculate_hours(start_time, end_time):
    """Calculate hours between punch in and punch out, handling night shifts."""
    seconds = ctime_diff_in_seconds(end_time, start_time)
    hours = seconds / 3600
    return round(hours, 2)




@frappe.whitelist(allow_guest=True)  # Allow external access if needed
def export_report_get(report_name, file_format_type="Excel", filters=None, visible_idx=None):
    """
    Custom API for exporting query reports using the GET method.

    Example Usage:
    https://yourdomain.com/api/method/custom_app.api.export_report_get?report_name=Visit Report&file_format_type=Excel&filters={"from_date":"2025-02-01","to_date":"2025-02-03"}
    """

    # Convert filter string to dictionary
    try:
        filters = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        frappe.throw("Invalid filters format. Must be a JSON object.")

    # Convert visible_idx to list if it's a string
    try:
        visible_idx = json.loads(visible_idx) if visible_idx else []
    except json.JSONDecodeError:
        frappe.throw("Invalid visible_idx format. Must be a JSON list.")

    # Check user permissions for exporting
    frappe.permissions.can_export(
        frappe.get_cached_value("Report", report_name, "ref_doctype"),
        raise_exception=True,
    )

    # Run the report and fetch data
    from frappe.desk.query_report import run
    data = run(report_name, filters, are_default_filters=False)
    data = frappe._dict(data)

    if not data.get("columns"):
        frappe.respond_as_web_page(
            "No data to export", "You can try changing the filters of your report."
        )
        return

    # Build XLSX data
    from frappe.desk.query_report import build_xlsx_data
    xlsx_data, column_widths = build_xlsx_data(data, visible_idx, include_indentation=False)

    # Handle different export formats
    file_extension = "csv" if file_format_type == "CSV" else "xlsx"

    if file_format_type == "CSV":
        csv_params = {}
        content = get_csv_bytes(xlsx_data, csv_params)
    elif file_format_type == "Excel":
        content = make_xlsx(xlsx_data, "Query Report", column_widths=column_widths).getvalue()
    else:
        frappe.throw(f"Invalid file format type: {file_format_type}. Use 'CSV' or 'Excel'.")

    # Provide the file as a binary response
    return provide_binary_file(report_name, file_extension, content)


import io
import xlsxwriter

@frappe.whitelist()
def export_report_to_excel(report_name, filters=None):
    """
    Generate and return an Excel file for the given report using `frappe.desk.query_report.run`.
    :param report_name: Name of the Frappe report
    :param filters: JSON string of filters
    :return: Binary file for download
    """

    # Convert filters from JSON string to dictionary
    filters = frappe.parse_json(filters) if filters else {}

    # Fetch report data and columns using `frappe.desk.query_report.run`
    try:
        report_data = frappe.call("frappe.desk.query_report.run", report_name=report_name, filters=filters)
        columns = report_data.get("columns", [])
        data = report_data.get("result", [])  # Query reports use "result" instead of "data"
    except Exception as e:
        frappe.throw(f"Error fetching report data: {str(e)}")

    # Create an in-memory Excel file
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output)
    worksheet = workbook.add_worksheet(report_name)

     # Load employee details using the filters
    employee_name = ""
    employee_code = ""
    designation = ""
    department = ""

    selected_month_str  = filters.get("time") or today()  # Get the selected month
    selected_month = datetime.strptime(selected_month_str, '%Y-%m-%d')

    start_date = datetime(selected_month.year, selected_month.month, 1)
    end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
   

    if filters.get("employee"):
        employee = frappe.get_doc("Employee", filters.get("employee"))
        employee_name = employee.employee_name
        employee_code = employee.name
        designation = employee.designation or ""
        department = employee.department or ""
        compnay  = employee.company

    # Set column widths
    for i, col in enumerate(columns):
        worksheet.set_column(i, i, col.get("width", 20) / 7)
    # Insert company logo and name
    logo_path = get_site_path("public", "files", "balavlogo.png")  # Adjust path as needed
    worksheet.insert_image("A1", logo_path, {"x_scale": 1, "y_scale": 1})

    title_format = workbook.add_format({"bold": True, "align": "center", "font_size": 16})
    worksheet.merge_range("B1:F1", compnay, title_format)

    # Report title
    date_range = f"From {start_date.strftime('%Y-%m-%d')} To {end_date.strftime('%Y-%m-%d')}"
    report_title = f"Punching In and Out Time {date_range}"
    subtitle_format = workbook.add_format({"align": "center", "font_size": 12})
    worksheet.merge_range("B2:F2", report_title, subtitle_format)

    # Employee details
    detail_format = workbook.add_format({"bold": True, "font_size": 10})
    normal_format = workbook.add_format({"font_size": 10})

    
    worksheet.merge_range("A4:B4", "Employee Name:", detail_format)
    worksheet.write("C4", employee_name, normal_format)
    worksheet.merge_range("A5:B5", "Employee Code:", detail_format)
    worksheet.write("C5", employee_code, normal_format)

    worksheet.write("E4", "Designation:", detail_format)
    worksheet.write("F4", designation, normal_format)
    worksheet.write("E5", "Department:", detail_format)
    worksheet.write("F5", department, normal_format)

    # Create header row for the table
    header_format = workbook.add_format({"bold": True, "border": 1, "bg_color": "#DDEBF7", "align": "center", "valign": "vcenter"})
    for col_num, col in enumerate(columns):
        worksheet.write(7, col_num, col["label"], header_format)

    # Write data with borders
    cell_format = workbook.add_format({"border": 1, "align": "center", "valign": "vcenter"})
    for row_num, row in enumerate(data, start=8):
        for col_num, col in enumerate(columns):
            worksheet.write(row_num, col_num, row.get(col["fieldname"], ""), cell_format)

    workbook.close()
    output.seek(0)

    # Prepare the response for binary file download
    safe_employee_name = employee_name.replace(" ", "_")
    frappe.response.filename = f"{report_name}_{safe_employee_name}.xlsx"
    frappe.response.filecontent = output.getvalue()
    frappe.response.type = "binary"
    frappe.response.headers = {
        "Content-Disposition": f'attachment; filename="{report_name}_{safe_employee_name}.xlsx"'
    }


def create_or_update_attendance(employee_id, attendance_date, working_hours,holidaystatus):
    existing_attendance = frappe.get_all("Attendance", 
                                        filters={"employee": employee_id, "attendance_date": attendance_date},
                                        fields=["name"])

    if working_hours >= 8:
        status = "Present"
    elif 4 <= working_hours < 8:
        status = "Half Day"
    else:
        status = "Absent"

    if holidaystatus==None:    
        if existing_attendance:
            frappe.db.set_value("Attendance", existing_attendance[0]["name"], {
                "status": status,
                "working_hours": working_hours
            })
        else:
            attendance = frappe.get_doc({
                "doctype": "Attendance",
                "employee": employee_id,
                "attendance_date": attendance_date,
                "status": status,
                "working_hours": working_hours
            })
            attendance.insert()
            attendance.submit()
    
    frappe.db.commit()


def create_attendance_record(employee_id, attendance_date, status,working_hours):
    """Creates or updates the attendance record for an employee."""
    attendance = frappe.get_value("Attendance", {"employee": employee_id, "attendance_date": attendance_date})

    if not attendance:
        doc = frappe.get_doc({
            "doctype": "Attendance",
            "employee": employee_id,
            "attendance_date": attendance_date,
            "working_hours":working_hours,
            "status": status,
        })
        doc.insert()
    else:
        frappe.db.set_value("Attendance", attendance, "status", status,"working_hours",working_hours)


# Scheduling (example in common_site_config.json):
# "scheduler_events": {
#     "daily": [
#         {"method": "your_module.your_file.mark_attendance", "frequency": "12:00 AM"}
#     ]
# }
@frappe.whitelist()
def get_citys(search=""):
    """
    Fetch documents sorted by city based on a search term.
    """
    search = search.strip()  # Strip any extra whitespace from the search term

    # Define the table name
    table = "tabcity"
    params = (f"%{search}%",)  # Wrap `search` in a tuple for parameterized query

    # Corrected SQL query
    query = f"""
    SELECT 
        name as id,
        city as name
    FROM 
        `{table}`
    WHERE 
        city LIKE %s
    ORDER BY 
        city
    LIMIT 20;
    """

    # Execute the query
    results = frappe.db.sql(query, params, as_dict=True)
    return results



@frappe.whitelist()
def get_sorted_leads_or_deals(lat, lon, search="", ctype="Lead"):
    """
    Fetch documents sorted by nearest distance based on search term or 10km radius.
    Includes records even if latitude or longitude is NULL or empty.
    """
    lat = float(lat)
    lon = float(lon)
    search = search.strip()  # Strip any extra whitespace from the search term

    # Determine table based on ctype
    table = "tabCRM Lead" if ctype == "Lead" else "tabCRM Deal"

    # Base SQL query
    query = f"""
    SELECT 
        name,
        lead_name,
        organization,
        custom_area,
        latitude,
        longitude,
        (
            6371 * ACOS(
                COS(RADIANS(%s)) * COS(RADIANS(latitude)) * 
                COS(RADIANS(longitude) - RADIANS(%s)) + 
                SIN(RADIANS(%s)) * SIN(RADIANS(latitude))
            )
        ) AS distance_km
    FROM 
        `{table}`
    """

    # Modify query based on whether a search term is provided
    params = (lat, lon, lat)  # Base parameters
    if search:  # If search term is provided
        query += """
        WHERE (
            lead_name LIKE %s OR
            organization LIKE %s
        )
        """
        params += (f"%{search}%", f"%{search}%")
    else:  # No search term, filter within 10km radius
        query += """
        HAVING distance_km <= 1
        """

    # Add sorting and limit
    query += """
    ORDER BY 
        distance_km
    LIMIT 20;
    """

    # Execute the query
    results = frappe.db.sql(query, params, as_dict=True)
    return results



@frappe.whitelist()
def get_employee_details():
    """
    Fetch employee details for the current date (all employees).

    Returns:
    - list: List of employee details with check-in, attendance, and employee info for today.
    """
    try:
        # Get current date
        today_date = datetime.today().date()
        
        # Define start and end times for the day
        start_time = datetime.combine(today_date, time.min)  # 00:00:00
        end_time = datetime.combine(today_date, time(23, 59, 59))  # 23:59:59
        
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        no_image_path = "/files/noimage.png" 

        # Fetch all employees
        employees = frappe.get_all("Employee", fields=["name", "employee_name","image"])
        employee_dict = {
            emp["name"]: {
                "employee_name": emp["employee_name"],
                "image": emp["image"] if emp["image"] else no_image_path
            }
            for emp in employees
        }
        
        # Fetch Employee Check-ins for today, grouped by employee (earliest check-in per employee)
        checkins = frappe.get_all(
            "Employee Checkin",
            filters={"time": ["BETWEEN", start_time_str, end_time_str]},
            fields=["employee", "MIN(time) as earliest_time"],
            group_by="employee"
        )
        
        # Fetch Attendance for today
        attendance = frappe.get_all(
            "Attendance",
            filters={"attendance_date": today_date},
            fields=["employee", "status"]
        )
        attendance_dict = {att["employee"]: att["status"] for att in attendance}
        
        # Prepare result list
        result = []
        for emp_id, emp_data in employee_dict.items():
            emp_name = emp_data["employee_name"]
            emp_img = emp_data["image"]
            # Check-in details
            checkin = next((c for c in checkins if c["employee"] == emp_id), None)
            if checkin and checkin["earliest_time"]:
                try:
                    checkin_time = datetime.strptime(str(checkin["earliest_time"]), '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    checkin_time = datetime.strptime(str(checkin["earliest_time"]), '%Y-%m-%d %H:%M:%S')
                time_str = checkin_time.strftime('%I:%M %p')  # 12-hour format
                checkin_message = f"Punched in at {time_str}"
            else:
                checkin_message = "No check-in recorded"
            
            # Attendance details
            attendance_status = attendance_dict.get(emp_id, "Absent")
            
            # Add to result
            result.append({
                "EmployeeId": emp_id,
                "EmployeeName": emp_name,
                "Emp_img":emp_img,
                "EmployeeCheckin": checkin_message,
                "Attendance": attendance_status
            })
        
        return result

    except Exception as e:
        frappe.log_error(f"Error in get_employee_details: {str(e)}", "Custom API Error")
        return {"error": str(e)}
    
def update_last_sync_time():
    # Get all Shift Type records with their primary keys (usually 'name' field)
    shift_types = frappe.get_all("Shift Type", fields=["name"])  # 'name' is usually the primary key in Frappe
    for shift in shift_types:
        # Update the 'last_sync_of_checkin' field by specifying the record ID
        frappe.db.set_value("Shift Type", shift['name'], "last_sync_of_checkin", now())

    frappe.db.commit()  # Ensure changes are committed to the database

@frappe.whitelist()
def get_location_logs():
    current_date = nowdate()
    logs = frappe.get_all(
        'Location Logs',
        filters={
            'date': current_date
        },
        fields=['latitude', 'longitude', 'timestamp'],
        rder_by="timestamp asc" 
    )
    return logs


@frappe.whitelist()
def get_todays_attendance_summary():
    """
    API to count today's attendance: Present, Absent, and other statuses.

    Returns:
        dict: A summary of today's attendance.
    """
    from frappe.utils import today

    # Get today's date
    current_date = today()

    # Fetch attendance records for today
    attendance_summary = frappe.db.sql(
        """
        SELECT
            status,
            COUNT(*) as count
        FROM
            `tabAttendance`
        WHERE
            attendance_date = %s
        GROUP BY
            status
        """,
        current_date,
        as_dict=True
    )

    # Structure the response
    summary = {record["status"]: record["count"] for record in attendance_summary}

    # Add default values if certain statuses are missing
    summary = {
        "Present": summary.get("Present", 0),
        "Absent": summary.get("Absent", 0),
        "On Leave": summary.get("On Leave", 0),
        "Half Day": summary.get("Half Day", 0),
    }

    return {
        "date": current_date,
        "summary": summary,
    }



@frappe.whitelist()
def get_leader_dashboard(filter_by="today", from_date=None, to_date=None, lbtype="visit"):
    """
    Leader dashboard API to get the top 5 employees ranked by the number of visits or tasks.

    Args:
        filter_by (str): Filter options - 'today', 'current_week', 'current_month', or 'custom'.
        from_date (str): Start date for custom filter (YYYY-MM-DD).
        to_date (str): End date for custom filter (YYYY-MM-DD).
        lbtype (str): Leaderboard type - 'visit' (default) or 'task'.

    Returns:
        dict: Leaderboard with top 5 employees ranked by the specified type (visits or tasks).
    """
    # Determine the date range based on the filter
    if filter_by == "today":
        from_date = frappe.utils.today()
        to_date = frappe.utils.today()
    elif filter_by == "current_week":
        from_date = frappe.utils.get_first_day_of_week(frappe.utils.today())
        to_date = frappe.utils.get_last_day_of_week(frappe.utils.today())
    elif filter_by == "current_month":
        from_date = frappe.utils.get_first_day(frappe.utils.today())
        to_date = frappe.utils.get_last_day(frappe.utils.today())
    elif filter_by == "custom":
        if not from_date or not to_date:
            frappe.throw("For custom filters, 'from_date' and 'to_date' are required.")
    else:
        frappe.throw("Invalid filter option. Choose from 'today', 'current_week', 'current_month', or 'custom'.")

    # Initialize leaderboard
    leaderboard = []
    no_image_path = "/files/noimage.png"

    if lbtype == "visit":
        # Fetch beat visit data and count visits by employee
        beat_visits = frappe.get_all(
            "Beat Visit",
            filters={
                "check_in_time": ["between", [from_date, to_date]],
            },
            fields=["employee", "count(name) as visit_count"],
            group_by="employee",
            order_by="visit_count desc",
            limit_page_length=10  # Limit results to top 5
        )

        # Prepare the leaderboard for visits
        for idx, visit in enumerate(beat_visits, start=1):
            #employee_name = frappe.db.get_value("Employee", visit["employee"], "employee_name")
            employee_details = frappe.db.get_value(
                "Employee",
                visit["employee"],
                ["employee_name", "image"],
                as_dict=True
            )
            
            employee_name = employee_details["employee_name"] if employee_details else "Unknown"
            employee_image = (
                employee_details["image"] if employee_details and employee_details["image"] else "/files/noimage.png"
            )

            leaderboard.append({
                "rank": idx,
                "employee": visit["employee"],
                "employee_name": employee_name,
                "employee_image": employee_image,
                "count": visit["visit_count"],
            })

    elif lbtype == "task":
        # Fetch CRM Task data and count tasks by employee
        tasks = frappe.get_all(
            "CRM Task",
            filters={
                "creation": ["between", [from_date, to_date]],
            },
            fields=["owner", "count(name) as task_count"],
            group_by="owner",
            order_by="task_count desc",
            limit_page_length=10  # Limit results to top 5
        )

        # Prepare the leaderboard for tasks
        for idx, task in enumerate(tasks, start=1):
            #employee_name = frappe.db.get_value("Employee", {"user_id": task["owner"]}, "employee_name")
            employee_details = frappe.db.get_value(
                "Employee",
                task["owner"],
                ["employee_name", "image"],
                as_dict=True
            )
            
            employee_name = employee_details["employee_name"] if employee_details else "Unknown"
            employee_image = (
                employee_details["image"] if employee_details and employee_details["image"] else "/files/noimage.png"
            )
            leaderboard.append({
                "rank": idx,
                "employee": task["owner"],
                "employee_name": employee_name or task["owner"],  # Use owner if no employee mapping
                "employee_image": employee_image,
                "count": task["task_count"],
            })

    else:
        frappe.throw("Invalid 'lbtype'. Choose between 'visit' and 'task'.")

    return {
        "from_date": from_date,
        "to_date": to_date,
        "leaderboard": leaderboard,
    }


@frappe.whitelist()
def get_Allbeat_visit(timestamp_filter=None):
    try:
        # Use today's date if no timestamp is provided
        if not timestamp_filter:
            timestamp_filter = nowdate()

        # Convert the single timestamp string to a Python date object
        date = getdate(timestamp_filter)
        from_date = to_date = date

        # Fetch Beat Visit records
        beat_visits = frappe.get_all(
            "Beat Visit",
            filters={
                "check_in_time": ["between", [from_date, to_date]],
            },
            fields=[
                "employee",
                "is_checked_out",
                "lead",
                "deal",
                "check_in_time",
                "check_out_time",
                "customer_interest",
                "latitude",
                "longitude",
            ]
        )
        no_image_path = "/files/noimage.png" 
        # Include linked document details for Beat Visits
        for visit in beat_visits:
            if visit.get("lead"):
                lead_doc = frappe.get_doc("CRM Lead", visit["lead"])
                visit['leadname'] = (lead_doc.salutation or "") + " " + (lead_doc.first_name or "") + "-" + (lead_doc.organization or "")
                visit["details"] = {
                    "organization": lead_doc.organization,
                    "salutation": (lead_doc.salutation or ""),
                    "first_name": lead_doc.first_name,
                    "Area":lead_doc.custom_area,
                    "image":lead_doc.image if getattr(lead_doc, "image", None) else no_image_path
                }
            if visit.get("deal"):
                deal_doc = frappe.get_doc("CRM Deal", visit["deal"])
                visit['leadname'] = (deal_doc.salutation or "") + " " + (deal_doc.first_name or "") + "-" + (deal_doc.organization or "")
                visit["details"] = {
                    "organization": deal_doc.organization,
                    "first_name": deal_doc.first_name,
                    "Area":lead_doc.custom_area,
                    "salutation": (deal_doc.salutation or ""),
                     "image":deal_doc.image if getattr(deal_doc, "image", None) else no_image_path
                }



        return beat_visits

    except Exception as e:
        frappe.log_error(message=str(e), title="Get Employee Timeline Error")
        frappe.throw(_("An error occurred while fetching the employee timeline: {0}").format(str(e)))

@frappe.whitelist()
def calculate_and_store_travel_databydate(employee=None, date=None):
    try:
        # If no date is provided, use today's date
        start_date = getdate(date) if date else getdate(nowdate())

        # If no employee is provided, fetch all active employees
        employees = [employee] if employee else frappe.get_all("Employee", filters={"status": "Active"}, pluck="name")

        for emp in employees:
            # Enqueue processing for each employee separately
            frappe.enqueue("cmenu.api.process_employee_travel_data", 
                           queue='long',  
                           employee=emp, 
                           start_date=start_date)

        frappe.msgprint(_("Travel data calculation has been queued for processing."))
    
    except Exception as e:
        frappe.log_error(message=str(e), title="Travel Data Calculation Error")



def process_employee_travel_data(employee, start_date):
    try:
        # Fetch traveled KM for the given employee and date
        travel_data = get_beat_visit_details(employee, start_date, start_date)
        
        # Extract total distance
        total_distance = travel_data.get("total_distance", "0 km").replace(" km", "")
        
        # Check if entry already exists to avoid duplicates
        existing_entry = frappe.get_all("Travel Data", filters={"employee": employee, "date": start_date}, pluck="name")

        if not existing_entry:
            # Store the data in Travel Data doctype
            frappe.get_doc({
                "doctype": "Travel Data",
                "employee": employee,
                "date": start_date,
                "km": float(total_distance)
            }).insert(ignore_permissions=True)

            frappe.db.commit()

        frappe.log_error(message=f"Processed travel data for {employee} on {start_date}", title="Travel Data Processed")

    except Exception as e:
        frappe.log_error(message=f"Error processing {employee}: {str(e)}", title="Travel Data Processing Error")


def calculate_and_store_travel_data():
    try:
        # Fetch the number of back days from Mobile App Settings
        mobile_settings = frappe.get_single("MobileAppSetting")
        back_days = mobile_settings.get("travel_back_days") or 0

        # Get today's date and calculate start date
        today = getdate(nowdate())
        start_date = add_days(today, -back_days)

        # Get all active employees
        employees = frappe.get_all("Employee", filters={"status": "Active"}, pluck="name")

        for employee in employees:
            current_date = start_date
            while current_date <= today:
                # Get traveled KM for the employee on that date
                travel_data = get_beat_visit_details(employee, current_date)

                # Get total traveled KM
                traveled_km = float(travel_data.get("total_distance", "0.0 km").replace(" km", ""))

                # Create or update the Travel Data record
                travel_entry = frappe.get_all(
                    "Travel Data",
                    filters={"employee": employee, "date": current_date},
                    fields=["name"]
                )

                if travel_entry:
                    # Update existing record
                    doc = frappe.get_doc("Travel Data", travel_entry[0]["name"])
                    doc.km = traveled_km
                    doc.save()
                else:
                    # Create a new record
                    doc = frappe.get_doc({
                        "doctype": "Travel Data",
                        "employee": employee,
                        "date": current_date,
                        "km": traveled_km
                    })
                    doc.insert()

                # Move to the next day
                current_date = add_days(current_date, 1)

        frappe.db.commit()

    except Exception as e:
        frappe.log_error(message=str(e), title="Error in Travel Data Calculation")


@frappe.whitelist()
def get_beat_visit_details(employee, timestamp_filter=None, to_date_filter=None):
    try:
        # Use today's date if no timestamp is provided
        if not timestamp_filter:
            timestamp_filter = nowdate()

        # Convert the single timestamp string to a Python date object
        from_date = getdate(timestamp_filter)

        # Use the provided end date or today's date if not provided
        if to_date_filter:
            to_date = getdate(to_date_filter)
        else:
            to_date = from_date

        # frappe.log_error(
        #     message=f"From Date: {from_date}, To Date: {to_date}", 
        #     title="Date Range"
        # )

        # Fetch Beat Visit records
        beat_visits = frappe.get_all(
            "Beat Visit",
            filters={
                "employee": employee,
                "check_in_time": ["between", [from_date, to_date]],
            },
            fields=[
                "employee",
                "is_checked_out",
                "lead",
                "deal",
                "check_in_time",
                "check_out_time",
                "customer_interest",
                "latitude",
                "longitude",
            ],
            order_by="check_in_time asc" 
        )

        # Include linked document details for Beat Visits
        for visit in beat_visits:
            visit_date = getdate(visit["check_in_time"])
            visit["visit_date"] = str(visit_date)  # Add visit_date
            if visit.get("lead"):
                lead_doc = frappe.get_doc("CRM Lead", visit["lead"])
                visit['leadname'] = (lead_doc.salutation or "") + " " + (lead_doc.first_name or "") + "-" + (lead_doc.organization or "")
                visit["details"] = {
                    "organization": lead_doc.organization,
                    "salutation": lead_doc.salutation,
                    "first_name": lead_doc.first_name,
                    "Area": lead_doc.custom_area
                }
            if visit.get("deal"):
                deal_doc = frappe.get_doc("CRM Deal", visit["deal"])
                visit['leadname'] = (deal_doc.salutation or "") + " " + (deal_doc.first_name or "") + "-" + (deal_doc.organization or "")
                visit["details"] = {
                    "organization": deal_doc.organization,
                    "first_name": deal_doc.first_name,
                    "salutation": deal_doc.salutation,
                    "Area": ""
                }

        # Fetch geo-tracking data for the employee on the given date
        geo_tracking = frappe.get_all(
            "Location Log",
            filters={
                "employee": employee,
                "accuracy":["<",15],
                "timestamp": ["between", [from_date, to_date]],
            },
            fields=[
                "timestamp",
                "latitude",
                "longitude",
                "speed",
                "activity_type"
            ],
            order_by="timestamp asc" 
        )

        timeline = []
        current_travel_segment = None
        total_distance = 0
        date_wise_distance = {}
        previous_entry = None
        still_count = 0

        # Process geo-tracking data for travel and stops
        for entry in geo_tracking:
            entry_date = getdate(entry["timestamp"])
            
            # Initialize distance for the date
            if entry_date not in date_wise_distance:
                date_wise_distance[entry_date] = 0.0

            # If the activity type is 'still', finalize the current travel segment (if any)
            if entry["activity_type"] == "still":
                still_count += 1
                if still_count >= 20 and current_travel_segment:
                    # Finalize the travel segment
                    current_travel_segment["end_time"] = format_time(previous_entry["timestamp"])
                    current_travel_segment["distance"] = f"{current_travel_segment['distance']:.2f} km"
                    timeline.append(current_travel_segment)
                    current_travel_segment = None
                    
            else:
                still_count = 0

            # If the activity type is 'in_vehicle', start or continue a travel segment
            if entry["activity_type"] == "in_vehicle":
                still_count = 0  # Reset still count
                if not current_travel_segment:
                    # Start a new travel segment
                    current_travel_segment = {
                        "type": "travel",
                        "start_time": format_time(entry["timestamp"]),
                        "visit_date": str(entry_date),  # Add visit_date
                        "distance": 0.0  # Initialize distance
                    }
                if previous_entry:
                    # Accumulate distance for the current travel segment
                    distance = calculate_distance(previous_entry, entry)
                    current_travel_segment["distance"] += distance   
                
            if previous_entry:
                # Accumulate distance for the current travel segment
                distance = calculate_distance(previous_entry, entry)
                total_distance += distance
                date_wise_distance[entry_date] += distance

            # Update the previous entry
            previous_entry = entry

        # Finalize the last travel segment (if any)
        if current_travel_segment:
            current_travel_segment["end_time"] = format_time(previous_entry["timestamp"])
            current_travel_segment["distance"] = f"{current_travel_segment['distance']:.2f} km"
            timeline.append(current_travel_segment)

        # Add planned visits (from Beat Visit records)
        for visit in beat_visits:
            timeline.append({
                "type": "planned_visit",
                "visit_time": f"{format_time(visit['check_in_time'])} - {format_time(visit['check_out_time'])}",
                "visit_date": visit["visit_date"],  # Add visit_date
                "location": f"Lat: {visit['latitude']}, Lon: {visit['longitude']}",
                "leadname": visit.get("leadname", ""),
                "details": visit.get("details", {}),
                "start_time": format_time(visit["check_in_time"])
            })
        # if to_date_filter:    
        #     total_distancebyfull = sum(date_wise_distance.values())
        # else:
        #     total_distancebyfull = calculate_total_distance(geo_tracking)
        timeline.sort(key=lambda x: x["start_time"])
        return {
            "timeline": timeline,
            "total_distance": f"{total_distance:.2f} km",
            "date_wise_distance": {
                str(date): f"{distance:.2f} km" for date, distance in date_wise_distance.items()
            }
        }

    except Exception as e:
        #frappe.log_error(message=str(e), title="Get Employee Timeline Error")
        frappe.throw(_("An error occurred while fetching the employee timeline: {0}").format(str(e)))

def calculate_total_distance(geo_tracking):
    """
    Calculate the total distance from all location entries in geo_tracking,
    without considering the activity type.

    Args:
        geo_tracking (list): List of geo-tracking entries containing latitude, longitude, and timestamp.

    Returns:
        float: Total distance in kilometers.
    """
    total_distance = 0.0
    previous_entry = None

    for entry in geo_tracking:
        if previous_entry:
            # Extract coordinates
            start_coords = (previous_entry["latitude"], previous_entry["longitude"])
            end_coords = (entry["latitude"], entry["longitude"])
            
            # Calculate the geodesic distance between points
            distance = geodesic(start_coords, end_coords).km
            
            # Accumulate total distance
            total_distance += distance
        
        # Update the previous entry
        previous_entry = entry

    return total_distance

def calculate_distance(start, end):
    start_coords = (start["latitude"], start["longitude"])
    end_coords = (end["latitude"], end["longitude"])
    return geodesic(start_coords, end_coords).km

@frappe.whitelist(allow_guest=True)
def create_crm_task():
    """
    Custom API to create a CRM Task based on JSON input from the request body.
    :return: JSON response with created task details or error message.
    """
    try:
        # Parse JSON payload from the request body
        task_data = frappe.request.get_json()
        if not task_data:
            frappe.throw(_("Invalid JSON payload."))

        # Validate required fields
        required_fields = [
            "title", "description", "status", "priority",
            "reference_doctype", "reference_docname",
            "due_date", "start_date", "assigned_to"
        ]

        for field in required_fields:
            if not task_data.get(field):
                frappe.throw(_(f"Field {field} is required."))

        # Validate Reference Doctype
        if not frappe.db.exists("DocType", task_data["reference_doctype"]):
            frappe.throw(_("Reference Doctype '{0}' does not exist.").format(task_data["reference_doctype"]))

        # Validate Reference Docname
        if not frappe.db.exists(task_data["reference_doctype"], task_data["reference_docname"]):
            frappe.throw(_("Reference Document '{0}' does not exist in {1}.")
                         .format(task_data["reference_docname"], task_data["reference_doctype"]))

        # Create the task
        task = frappe.get_doc({
            "doctype": "CRM Task",
            "title": task_data["title"],
            "description": task_data["description"],
            "status": task_data["status"],
            "priority": task_data["priority"],
            "reference_doctype": task_data["reference_doctype"],
            "reference_docname": task_data["reference_docname"],
            "due_date": task_data["due_date"],
            "start_date": task_data["start_date"],
            "assigned_to": task_data["assigned_to"]
        })

        # Save the task
        task.insert()
        frappe.db.commit()

        return {
            "status": "success",
            "message": "CRM Task created successfully.",
            "data": task.as_dict()
        }

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "CRM Task Creation Failed")
        return {
            "status": "error",
            "message": str(e)
        }


@frappe.whitelist(allow_guest=True)
def store_location_log():
    try:
        # Use 4 spaces for indentation
        data = frappe.request.get_data(as_text=True)
        #frappe.log_error("location",data)
        location_data = json.loads(data)
        # The data you need is under the "location" key
        location = location_data.get("location", {})

        # Extract required fields
        uuid = location.get("uuid")
        coords = location.get("coords", {})
        activity = location.get("activity", {})
        battery = location.get("battery", {})
        provider = location.get("provider", {})
        isgps = provider.get("gps")
        accuracyauthorization = provider.get("accuracyAuthorization")
        isproviderchangeevent = False
        if provider:
            isproviderchangeevent = True
        
        latitude = coords.get("latitude")
        longitude = coords.get("longitude")
        accuracy = coords.get("accuracy")
        speed = coords.get("speed")
        raw_timestamp = location.get("timestamp")
        if raw_timestamp.endswith("Z"):
           raw_timestamp = raw_timestamp.replace("Z", "+00:00")  # Replace 'Z' with '+00:00'

        utc_time = datetime.fromisoformat(raw_timestamp).replace(tzinfo=utc)  # Parse and assign UTC timezone
        ist_time = utc_time.astimezone(timezone('Asia/Kolkata'))  # Convert to IST timezone
        timestamp = ist_time.strftime('%Y-%m-%d %H:%M:%S')  # Format the IST timestamp

        # IST_OFFSET = timedelta(hours=5, minutes=30)
        # IST = timezone(IST_OFFSET)
        # ist_time = datetime.fromisoformat(raw_timestamp).replace(tzinfo=IST)
        # timestamp = ist_time.strftime('%Y-%m-%d %H:%M:%S')
        employee_id = location_data.get("location", {}).get("extras", {}).get("employee_id")

        # Insert data into the custom Doctype
        doc = frappe.get_doc({
            "doctype": "Location Log",
            "uuid": uuid,
            "latitude": latitude,
            "longitude": longitude,
            "accuracy": accuracy,
            "speed": speed,
            "timestamp": timestamp,
            "activity_type": activity.get("type"),
            "battery_level": battery.get("level"),
            "is_moving": location.get("is_moving"),
            "is_charging": battery.get("is_charging"),
            "employee":employee_id,
            "is_gps":isgps,
            "accuracyauthorization":accuracyauthorization,
            "isproviderchangeevent":isproviderchangeevent,
        })
        doc.insert(ignore_permissions=True)
        frappe.db.commit()

        return {"status": "success", "message": "Location log saved successfully"}
    
    except Exception as e:
        frappe.log_error(f"Error in store_location_log: {str(e)}", "Background Location Log Error")
        return {"status": "error", "message": str(e)}



@frappe.whitelist()
def get_employee_checkins_and_attendance(employee_id, date=None):
    if not date:
        date = nowdate()  # Default to today's date if none provided
    
    date = getdate(date)  # Ensure date is in date format

    # Fetch attendance for the specified date
    attendance = frappe.get_all(
        "Attendance",
        filters={
            "employee": employee_id,
            "attendance_date": date,
            "docstatus": 1  # Only submitted attendance records
        },
        fields=["attendance_date", "status", "in_time", "out_time", "late_entry"]
    )

    # Fetch employee check-ins for the specified date
    checkins = frappe.get_all(
        "Employee Checkin",
        filters={
            "employee": employee_id,
            "time": ["between", (f"{date} 00:00:00", f"{date} 23:59:59")]
        },
        fields=["time", "log_type"]
    )

    return {
        "attendance": attendance,
        "checkins": checkins
    }

@frappe.whitelist()
def get_attn_counts(parent_employee):
    # Function to fetch count of attendance types and total employees
    def fetch_attendance_counts(employee_id):
        # Initialize counts
        counts = {
            "total_non_marked": 0,
            "total_absent": 0,
            "total_late": 0,
            "total_present": 0
        }
        
        # Check today's attendance records
        attendance_records = frappe.get_all(
            "Attendance",
            filters={
                "employee": employee_id,
                "attendance_date": nowdate(),
                "docstatus": 1  # Only submitted attendance records
            },
            fields=["status", "late_entry"]
        )

        # Classify attendance statuses
        if attendance_records:
            for record in attendance_records:
                if record["status"] == "Absent":
                    counts["total_absent"] += 1
                elif record["status"] == "Present":
                    counts["total_present"] += 1
                    # Increment late count if "Late Entry" is checked
                    if record.get("late_entry"):
                        counts["total_late"] += 1
        else:
            counts["total_non_marked"] += 1  # No attendance marked for today

        return counts

    # Recursive function to fetch hierarchy and aggregate attendance counts
    def fetch_hierarchy_counts(employee_id):
        # Get direct reports for the given employee
        direct_reports = frappe.get_all(
            "Employee",
            filters={"reports_to": employee_id},
            fields=["name"]
        )
        
        # Aggregate counts for all direct reports and their subordinates
        aggregated_counts = {
            "total_non_marked": 0,
            "total_absent": 0,
            "total_late": 0,
            "total_present": 0,
            "total_employees": 1,  # Include the current employee
            "is_end_employee": False  # Default to False
        }
        
        # If no direct reports, mark as end employee and fetch attendance counts
        if not direct_reports:
            aggregated_counts["is_end_employee"] = True
            employee_counts = fetch_attendance_counts(employee_id)
            for key, value in employee_counts.items():
                aggregated_counts[key] += value
            return aggregated_counts

        # For each report, recursively fetch their attendance counts
        for report in direct_reports:
            report_counts = fetch_hierarchy_counts(report["name"])
            for key, value in report_counts.items():
                if key != "is_end_employee":  # Avoid adding the end-employee flag
                    aggregated_counts[key] += value
        
        return aggregated_counts

    # Check if the provided ID is an Employee
    if frappe.db.exists("Employee", parent_employee):
        # Fetch attendance counts if the user is an employee
        return fetch_hierarchy_counts(parent_employee)
    
    # If the user is a System User, calculate counts for all employees
    elif frappe.db.exists("User", parent_employee):
        all_employees = frappe.get_all("Employee", fields=["name"])
        
        # Initialize the aggregate counts
        total_counts = {
            "total_non_marked": 0,
            "total_absent": 0,
            "total_late": 0,
            "total_present": 0,
            "total_employees": len(all_employees)
        }

        # Sum up attendance counts for all employees
        for emp in all_employees:
            employee_counts = fetch_attendance_counts(emp["name"])
            for key, value in employee_counts.items():
                total_counts[key] += value
        
        return total_counts

    else:
        # Return an error if the provided ID is neither an Employee nor a User
        frappe.throw("The provided ID does not correspond to an Employee or User.")

@frappe.whitelist()
def get_early_Dashbord(parent_employee, date=None):
    if not date:
        date = nowdate()  # Default to today's date if none provided
    
    date = getdate(date)  # Ensure date is in date format

    # Function to fetch today's late entry attendance and check-ins for a given employee
    def fetch_late_entry_attendance_and_checkins(employee_id):
        # Fetch attendance with late entry for today
        attendance = frappe.get_all(
            "Attendance",
            filters={
                "employee": employee_id,
                "attendance_date": date,
                "docstatus": 1,       # Only submitted attendance records
                "early_exit": 1       # Only late entries
            },
            fields=["attendance_date", "status", "in_time", "out_time"]
        )
        
        # Fetch check-ins for today
        checkins = frappe.get_all(
            "Employee Checkin",
            filters={
                "employee": employee_id,
                "time": ["between", (f"{date} 00:00:00", f"{date} 23:59:59")]
            },
            fields=["time", "log_type"]
        )
        
        # Fetch employee image or set a default placeholder
        employee_image = frappe.db.get_value("Employee", employee_id, "image") or "/files/noimage.png"
        
        # Only return data if there is a late entry attendance
        if attendance:
            return {"attendance": attendance, "checkins": checkins, "image": employee_image}
        return None

    # Function to recursively fetch the employee hierarchy
    def fetch_hierarchy(employee_id):
        # Get direct reports of the given employee
        direct_reports = frappe.get_all(
            "Employee",
            filters={"reports_to": employee_id},
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # If the employee has no direct reports, check if they have late entry attendance
        if not direct_reports:
            # Fetch attendance and check-in details if they have no children
            return {
                "name": employee_id,
                **fetch_late_entry_attendance_and_checkins(employee_id)
            } if fetch_late_entry_attendance_and_checkins(employee_id) else None
        
        # For each direct report, fetch their hierarchy and late entry attendance
        children = []
        for report in direct_reports:
            report_data = fetch_hierarchy(report["name"])
            if report_data:
                report["children"] = report_data
                report["attendance"] = frappe.get_all(
                    "Attendance",
                    filters={
                        "employee": report["name"],
                        "attendance_date": date,
                        "docstatus": 1,
                        "late_entry": 1     # Only late entries
                    },
                    fields=["attendance_date", "status", "in_time", "out_time"]
                )
                report["image"] = report["image"] or "No Image"  # Set "No Image" if no image
                children.append(report)
        
        return children if children else None

    # Check if the provided ID is an Employee
    if frappe.db.exists("Employee", parent_employee):
        # Fetch the hierarchy if the user is an employee
        return fetch_hierarchy(parent_employee)
    
    # If the user is not an Employee, check if they are a System User
    elif frappe.db.exists("User", parent_employee):
        # Fetch all employees under the company and their attendance
        all_employees = frappe.get_all(
            "Employee",
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # For each employee, add their late entry attendance and check-in details
        late_entry_employees = []
        for emp in all_employees:
            emp_data = fetch_late_entry_attendance_and_checkins(emp["name"])
            if emp_data:
                emp.update(emp_data)
                emp["image"] = emp["image"] or "No Image"  # Set "No Image" if no image
                late_entry_employees.append(emp)
        
        return late_entry_employees

    else:
        # Return an error if the provided ID is neither an Employee nor a User
        frappe.throw("The provided ID does not correspond to an Employee or User.")



@frappe.whitelist()
def get_late_Dashbord(parent_employee, date=None):
    if not date:
        date = nowdate()  # Default to today's date if none provided
    
    date = getdate(date)  # Ensure date is in date format

    # Function to fetch today's late entry attendance and check-ins for a given employee
    def fetch_late_entry_attendance_and_checkins(employee_id):
        # Fetch attendance with late entry for today
        attendance = frappe.get_all(
            "Attendance",
            filters={
                "employee": employee_id,
                "attendance_date": date,
                "docstatus": 1,       # Only submitted attendance records
                "late_entry": 1       # Only late entries
            },
            fields=["attendance_date", "status", "in_time", "out_time"]
        )
        
        # Fetch check-ins for today
        checkins = frappe.get_all(
            "Employee Checkin",
            filters={
                "employee": employee_id,
                "time": ["between", (f"{date} 00:00:00", f"{date} 23:59:59")]
            },
            fields=["time", "log_type"]
        )
        
        # Fetch employee image or set a default placeholder
        employee_image = frappe.db.get_value("Employee", employee_id, "image") or "/files/noimage.png"
        
        # Only return data if there is a late entry attendance
        if attendance:
            return {"attendance": attendance, "checkins": checkins, "image": employee_image}
        return None

    # Function to recursively fetch the employee hierarchy
    def fetch_hierarchy(employee_id):
        # Get direct reports of the given employee
        direct_reports = frappe.get_all(
            "Employee",
            filters={"reports_to": employee_id},
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # If the employee has no direct reports, check if they have late entry attendance
        if not direct_reports:
            # Fetch attendance and check-in details if they have no children
            return {
                "name": employee_id,
                **fetch_late_entry_attendance_and_checkins(employee_id)
            } if fetch_late_entry_attendance_and_checkins(employee_id) else None
        
        # For each direct report, fetch their hierarchy and late entry attendance
        children = []
        for report in direct_reports:
            report_data = fetch_hierarchy(report["name"])
            if report_data:
                report["children"] = report_data
                report["attendance"] = frappe.get_all(
                    "Attendance",
                    filters={
                        "employee": report["name"],
                        "attendance_date": date,
                        "docstatus": 1,
                        "late_entry": 1     # Only late entries
                    },
                    fields=["attendance_date", "status", "in_time", "out_time"]
                )
                report["image"] = report["image"] or "No Image"  # Set "No Image" if no image
                children.append(report)
        
        return children if children else None

    # Check if the provided ID is an Employee
    if frappe.db.exists("Employee", parent_employee):
        # Fetch the hierarchy if the user is an employee
        return fetch_hierarchy(parent_employee)
    
    # If the user is not an Employee, check if they are a System User
    elif frappe.db.exists("User", parent_employee):
        # Fetch all employees under the company and their attendance
        all_employees = frappe.get_all(
            "Employee",
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # For each employee, add their late entry attendance and check-in details
        late_entry_employees = []
        for emp in all_employees:
            emp_data = fetch_late_entry_attendance_and_checkins(emp["name"])
            if emp_data:
                emp.update(emp_data)
                emp["image"] = emp["image"] or "No Image"  # Set "No Image" if no image
                late_entry_employees.append(emp)
        
        return late_entry_employees

    else:
        # Return an error if the provided ID is neither an Employee nor a User
        frappe.throw("The provided ID does not correspond to an Employee or User.")


@frappe.whitelist()
def get_attn_Dashbord(parent_employee, date=None):
    if not date:
        date = nowdate()  # Default to today's date if none provided
    
    date = getdate(date)  # Ensure date is in date format

    # Function to fetch today's attendance and check-ins for a given employee
    def fetch_attendance_and_checkins(employee_id):
        # Fetch attendance for today
        attendance = frappe.get_all(
            "Attendance",
            filters={
                "employee": employee_id,
                "attendance_date": date,
                "docstatus": 1  # Only submitted attendance records
            },
            fields=["attendance_date", "status", "in_time", "out_time"]
        )
        
        # Fetch check-ins for today
        checkins = frappe.get_all(
            "Employee Checkin",
            filters={
                "employee": employee_id,
                "time": ["between", (f"{date} 00:00:00", f"{date} 23:59:59")]
            },
            fields=["time", "log_type"]
        )
        
        # Fetch employee image or set a default placeholder
        employee_image = frappe.db.get_value("Employee", employee_id, "image") or "/files/noimage.png"
        
        return {"attendance": attendance, "checkins": checkins, "image": employee_image}

    # Function to recursively fetch the employee hierarchy
    def fetch_hierarchy(employee_id):
        # Get direct reports of the given employee
        direct_reports = frappe.get_all(
            "Employee",
            filters={"reports_to": employee_id},
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # Check if the employee has no direct reports
        if not direct_reports:
            # Fetch attendance and check-in details if they have no children
            return {
                "name": employee_id,
                **fetch_attendance_and_checkins(employee_id)
            }
        
        # For each direct report, fetch their hierarchy and attendance
        for report in direct_reports:
            report["children"] = fetch_hierarchy(report["name"])
            report["attendance"] = frappe.get_all(
                "Attendance",
                filters={
                    "employee": report["name"],
                    "attendance_date": date,
                    "docstatus": 1
                },
                fields=["attendance_date", "status", "in_time", "out_time"]
            )
            report["image"] = report["image"] or "No Image"  # Set "No Image" if no image
        
        return direct_reports

    # Check if the provided ID is an Employee
    if frappe.db.exists("Employee", parent_employee):
        # Fetch the hierarchy if the user is an employee
        return fetch_hierarchy(parent_employee)
    
    # If the user is not an Employee, check if they are a System User
    elif frappe.db.exists("User", parent_employee):
        # Fetch all employees under the company and their attendance
        all_employees = frappe.get_all(
            "Employee",
            fields=["name", "employee_name", "designation", "department", "image"]
        )
        
        # For each employee, add their attendance and check-in details
        for emp in all_employees:
            emp.update(fetch_attendance_and_checkins(emp["name"]))
            emp["image"] = emp["image"] or "No Image"  # Set "No Image" if no image
        
        return all_employees

    else:
        # Return an error if the provided ID is neither an Employee nor a User
        frappe.throw("The provided ID does not correspond to an Employee or User.")


@frappe.whitelist()
def get_beat_visitsDB(filters=None):
    """
    Fetch Beat Visit data with additional details for linked Lead and Deal fields.
    
    :param filters: JSON string of filters to apply.
    :return: List of Beat Visit records with expanded Lead and Deal details.
    """
    import json

    # Ensure filters are provided
    if not filters:
        frappe.throw(_("Filters are mandatory."))

    try:
        # Parse filters from JSON string
        filters = json.loads(filters)
    except Exception:
        frappe.throw(_("Invalid filters format. Provide a valid JSON string."))

    # Fetch Beat Visit records based on filters
    beat_visits = frappe.get_all(
        "Beat Visit",
        filters=filters,
        fields=[
            "name",
            "employee",
            "is_checked_out",
            "lead",
            "deal",
            "check_in_time",
            "check_out_time",
            "customer_interest",
            "latitude",
            "longitude",
        ],
    )

    # Add additional details for Lead and Deal fields
    for visit in beat_visits:
        if visit.get("lead"):
            visit["lead_details"] = frappe.get_value(
                "CRM Lead",
                visit["lead"],
                ["lead_name", "status", "organization", "territory"],
                as_dict=True,
            )
        if visit.get("deal"):
            visit["deal_details"] = frappe.get_value(
                "CRM Deal",
                visit["deal"],
                ["lead_name", "status", "territory"],
                as_dict=True,
            )

    return {"data": beat_visits}


@frappe.whitelist(allow_guest=True)
def hr_login(email, password):
    try:
        # Check if the user exists
        if "@" in email:
            user = frappe.get_doc("User", email)
        else:
            # Assume it's a phone number; find the user with that phone number
            user = frappe.get_all(
                "User",
                filters={"mobile_no": email},
                fields=["name", "email"],
                limit_page_length=1
            )
            if user:
                user = frappe.get_doc("User", user[0]["email"])
            else:
                frappe.throw("Invalid login credentials", frappe.AuthenticationError)

        # Validate the password using frappe.utils.password.check_password
        frappe.utils.password.check_password(user.name, password)

        # Generate new API secret every time upon login
        api_key = user.api_key or frappe.generate_hash(length=15)
        api_secret = frappe.generate_hash(length=15)  # Generate new secret
        user.api_key = api_key
        user.api_secret = api_secret  # Store the new secret
        user.save(ignore_permissions=True)

        # Fetch user roles
        roles = [role.role for role in user.get("roles")]

        # Initialize the employee details as None
        employee_details = None

        # Check if the user is linked to an Employee record
        employee = frappe.db.get_value("Employee", {"user_id": user.name}, ["name", "employee_name", "designation", "department", "company","image"])

        if employee:
            # If user is an employee, fetch employee details
            employee_details = {
                "employee_id": employee[0],
                "employee_name": employee[1],
                "designation": employee[2],
                "department": employee[3],
                "company": employee[4],
		"employee_photo":employee[5] if employee[5] else "/files/noimage.png",
            }

        # Construct response
        response = {
            "status": "success",
            "message": "Login successful",
            "api_key": api_key,
            "api_secret": api_secret,  # Return the new plain text secret
            "user_email": user.email,
            "roles": roles,
            "full_name": user.full_name,
            "user_id": user.name,
            "employee_details": employee_details  # Include employee details if available
        }
        return response

    except frappe.AuthenticationError:
        frappe.throw(_("Invalid email or password"), frappe.AuthenticationError)
    except frappe.DoesNotExistError:
        frappe.throw(_("User does not exist"), frappe.AuthenticationError)
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Custom Login API Error")
        return {"status": "error", "message": str(e)}
    
@frappe.whitelist()
def get_meta():
    m = frappe.form_dict['doctype']
    data = frappe.get_meta(m)
    return data
    
@frappe.whitelist()
def get_user_menu():
    """Fetches menus based on the user roles"""
    roles = frappe.get_roles(frappe.session.user)
    menu_items = frappe.get_all(
        "Menu",
        filters={"role": ["in", roles], "is_active": 1},
        fields=["menu_name", "parent_menu", "menu_type", "cmenudoctype", "dashboard", "url","icon"],
 	order_by="orderno asc" 
    )
    return build_menu_tree(menu_items)

def build_menu_tree(menu_items):
    """Builds a nested list structure for menus."""
    menu_dict = {}
    menu_list = []

    # Create a dictionary for quick lookup
    for item in menu_items:
        menu_dict[item['menu_name']] = {
            'name': item['menu_name'],
            'link': get_menu_link(item),
            'icon':item['icon'],		
            'children': []
        }

    # Build the menu structure
    for item in menu_items:
        if item['parent_menu']:
            # If there's a parent, add to the parent's children
            parent_name = item['parent_menu']
            if parent_name in menu_dict:
                menu_dict[parent_name]['children'].append(menu_dict[item['menu_name']])
        else:
            # If no parent, it's a top-level menu item
            menu_list.append(menu_dict[item['menu_name']])

    return menu_list

def get_menu_link(menu_item):
    """Return the appropriate URL based on the menu type, formatted correctly."""
    if menu_item['menu_type'] == "DocType":
        # Convert to lowercase and replace spaces with hyphens
        doc_type_slug = menu_item['cmenudoctype'].replace(" ", "-").lower()
        return f"/app/{doc_type_slug}"
    elif menu_item['menu_type'] == "Dashboard":
        return f"/app/dashboard/{menu_item['dashboard']}"
    elif menu_item['menu_type'] == "Custom URL":
        return menu_item['url']
    return "#"        

    # Build the menu structure
    for item in menu_items:
        if item['parent_menu']:
            # If there's a parent, add to the parent's children
            parent_name = item['parent_menu']
            if parent_name in menu_dict:
                menu_dict[parent_name]['children'].append(menu_dict[item['menu_name']])
        else:
            # If no parent, it's a top-level menu item
            menu_list.append(menu_dict[item['menu_name']])

    return menu_list

def get_menu_link(menu_item):
    """Return the appropriate URL based on the menu type, formatted correctly."""
    if menu_item['menu_type'] == "DocType":
        # Convert to lowercase and replace spaces with hyphens
        doc_type_slug = menu_item['cmenudoctype'].replace(" ", "-").lower()
        return f"/app/{doc_type_slug}"
    elif menu_item['menu_type'] == "Dashboard":
        return f"/app/dashboard/{menu_item['dashboard']}"
    elif menu_item['menu_type'] == "Custom URL":
        return menu_item['url']
    return "#"