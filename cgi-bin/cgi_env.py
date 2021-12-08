#!/usr/bin/python3

import os

print ("Content-type: text/html")
print ()
print ("<meta charset=\"utf-8\">")
print ("env<br>")
print ("<ul>")
for key in os.environ.keys():
    print ("<li><span style='color:green'>%30s </span> : %s </li>" % (key,os.environ[key]))
print ("</ul>")
