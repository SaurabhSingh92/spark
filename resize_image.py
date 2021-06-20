from tkinter import *
from tkinter import filedialog
from PIL import Image
import os
root = Tk()

def img_small():
    opath = filedialog.askopenfilename()
    path = os.path.dirname(opath)
    print("file selected")
    im = Image.open(fr"{opath}")
    print("File compress")
    im.thumbnail((450, 500))
    im.save(f'{path}/sign_thumbnail.jpeg')
    print("Created")

button = Button(root, text="Submit Image", command=img_small)
button.pack()
root.mainloop()