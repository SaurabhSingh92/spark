from tkinter import *
from tkinter import ttk, filedialog, messagebox

# -----------------------------------------------------------------------------------------------#
# Create by - Saurabh Singh
# Usage - Auto generate various gcp artifacts required for implementing data lake/ cloud migration
# Verions       Date        User                Comment
#    1.0    29 May 2021     Saurabh Singh       Initial Draft
#
#
# ------------------------------------------------------------------------------------------------#

root = Tk()
# Window function
root.title("GCP Generator")
root.geometry('800x600')
# Tab windows
tab_control = ttk.Notebook(root)
ext = ttk.Frame(tab_control)
ref = ttk.Frame(tab_control)
tab_control.add(ext, text="External Table")
tab_control.add(ref, text="Refinement Table")

def getpath():
    if not ref_tbl_name.get():
        messagebox.showerror("Error", "Please fill mandatory details")
        exit(-1)
    global ref_path
    ref_path = filedialog.askdirectory()
    if not ref_path:
        messagebox.showerror("Error", "Select correct output path")
        exit(-1)
    out_loc['text'] = f"Location saved: \n\b{ref_path}"
    out_loc['state'] = "disabled"
    if ref_db_nm.get() and ref_tbl_name.get():
        sel_excel['state'] = "normal"
        ref_ddl['state'] = "normal"
    else:
        messagebox.showerror("Parameter Missing", "Fill Mandatory columns")


def genddl():
    print("Generating External ddl:")
    tableName = table_name.get()
    database = database_nm.get()
    gcs_path = prq_gs_path.get()
    # check path
    if not (tableName and gcs_path):
        messagebox.showerror("Error", "Please fill the mandatory columns")
        exit(-1)

    messagebox.showinfo("Output Path", "Please select where generated dll should be kept")
    opath = filedialog.askdirectory()

    if not opath:
        messagebox.showerror("Error", "Select correct output path")
        exit(-1)

    extdll = """,
    {""" + f"""
    CREATE EXTERNAL TABLE {database}.{tableName}
    WITH PARTITION COLUMNS 
    OPTIONS( 
        uris = [\"{gcs_path}/*.parquet\"]
        hive_partitioned_uri_prefix = \"{gcs_path}\"
    )
    """ + """label = { 
            team = 'eiger'
            }
    """
    open(f"{opath}/{tableName}_ddl.txt", "w").write(extdll)
    messagebox.showinfo("Success", f"File created successfully \n {opath}/{tableName}_ddl.txt")
    create_ddl['text'] = "DDL Generated"
    create_ddl['state'] = "disabled"


def gen_schema():
    print("Start generating the schema")
    messagebox.showinfo("Schema file", "Click to select excel file")
    file = filedialog.askopenfilename()
    print(f"{file}")
    messagebox.askyesno("Run", "Are you sure you want to create?")
    messagebox.showinfo("Success", f"Schema File created \n {ref_path}/{ref_tbl_name.get()}")
    sel_excel['text'] = "File Selected"
    sel_excel['state'] = "disabled"

def gen_ref_ddl():
    print("Start generating the refinement table ddl")


# External Tables
file_path = Label(ext, text="Enter the parquet GCS bucket location*")
prq_gs_path = Entry(ext, width=50)
ext_database = Label(ext, text="External Database Name ")
database_nm = Entry(ext, width=50)
ext_name = Label(ext, text="External Table name* ")
table_name = Entry(ext, width=50)
create_ddl = Button(ext, text="Click to generate external table DDL", command=genddl)

# External Window positioning
file_path.grid(row=0, column=0)
prq_gs_path.grid(row=0, column=1)
ext_database.grid(row=1, column=0)
database_nm.grid(row=1, column=1)
ext_name.grid(row=2, column=0)
table_name.grid(row=2, column=1)
create_ddl.grid(row=3, column=1)

# Refinement table
ref_database = Label(ref, text="Enter the refinement table database name")
ref_db_nm = Entry(ref, width=50)
ref_table = Label(ref, text="Enter the refinement table name*")
ref_tbl_name = Entry(ref, width=50)
out_loc = Button(ref, text="Click to select path where to create file", command=getpath)
sel_excel = Button(ref, text="Import excel with schema details ", command=gen_schema, state="disabled")
ref_ddl = Button(ref, text="Click to generate refinement table ddl", command=gen_ref_ddl, state="disabled")

# Refinement Window positioning
ref_database.grid(row=0, column=0)
ref_db_nm.grid(row=0, column=1)
ref_table.grid(row=1, column=0)
ref_tbl_name.grid(row=1, column=1)
out_loc.grid(row=2, column = 1)
sel_excel.grid(row=3, column=1)
ref_ddl.grid(row=4, column=1)

# Final Loop
tab_control.pack(expand=1, fill='both')
root['bg'] = '#49A'
root.mainloop()
