from openpyxl import load_workbook

# test_excel_anchorbase.xlsx


def get_data(p_file: str, p_worksheet =None, p_columns: list =None, p_header: bool =False, p_first_row: int =1):
    """
    Read the data from excel

    :param p_file: file path and file name
    :param p_worksheet: worksheet name. If worksheet isn't defined, all worksheets will be read
    :param p_columns: list of columns. If columns aren't defined, only the first cell will be read
    :param p_first_row: the number of a table first row
    :param p_header: table has a header or not
    :return: tuple of data or error
    """
    try:
        l_workbook=load_workbook(filename = p_file, read_only=True)
        if not p_worksheet:
            l_worksheet_name_list=l_workbook.sheetnames
        else:
            l_worksheet_name_list=[p_worksheet]
        for i_worksheet_name in l_worksheet_name_list: # read the worksheet/all worksheets
            l_worksheet=l_workbook[i_worksheet_name]
            if not p_columns: # if column aren't defined, the first cell will be read. Can be used to check a file/worksheet existing
                return l_worksheet['A1'].value, None
            # determine the table in a worksheet
            l_table=l_worksheet[p_first_row:l_worksheet.max_row]
            # transform the data into list of tuples
            l_data=[]
            # determine the value of every cell in the first row
            l_cell_number_dict={}
            for i, i_cell in enumerate(l_table[0]):
                l_cell_number_dict.update({i_cell.value:i})
            for i, i_row in enumerate(l_table):
                if p_header and i==0:
                    continue
                l_row=[]
                for i_column in p_columns:
                    if type(i_column)==int:
                        if i_column>i_row.__len__():
                            return None, 'There is no '+str(i_column)+'-th column'
                        l_row.append(i_row[i_column-1].value)
                    else:
                        l_cell_number=l_cell_number_dict.get(i_column)
                        if not l_cell_number:
                            return None, 'There is no '+i_column+" column"
                        else:
                            l_row.append(i_row[l_cell_number_dict.get(i_column)].value)
                l_data.append(tuple(l_row))
            return l_data, None
    except Exception as e:
        l_error=str(e)
        return None, l_error
