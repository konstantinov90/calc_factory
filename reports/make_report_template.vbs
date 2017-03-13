option explicit

dim excel
dim template_name 'the directory for storing files
dim cells
dim current_work_sheet

template_name = "{template_name}"

set excel = createobject("excel.application")

excel.Workbooks.open template_name

set current_work_sheet = excel.ActiveWorkbook.Worksheets("{worksheet}")

set cells = current_work_sheet.Cells
'cells(1,1).Value = "Test"
{data}

'script to enter data(into excel sheet)

excel.ActiveWorkbook.SaveAs ("{report_name}")
excel.ActiveWorkbook.Close
excel.Application.Quit
excel.Quit

Set current_work_sheet = Nothing

'filesys.CopyFile template_name & filename, "D:\kedar\"

set excel = nothing
