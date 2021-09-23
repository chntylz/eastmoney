#!/usr/bin/env python
#coding:utf-8
import os,sys
import datetime


nowdate=datetime.datetime.now().date()
#nowdate=nowdate-datetime.timedelta(1)
src_dir=nowdate.strftime("%Y-%m-%d")
stock_data_dir="stock_data"
file_name='zig'
target_html=file_name + '-index.html'

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir)
    # print("p0 :%s" % (files))
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    print("p1 :%s"%(images))
    images=[item[item.rfind('/')+1:] for item in images]
    print("p3 :%s"%(images))
    newfile='%s/%s'%(savedir, target_html)
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s-index </title>\n' % file_name)
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}\n')
        f.write('\n')
        f.write('\n')

        f.write('/* gridtable */\n')
        f.write('table.gridtable {\n')
        #f.write('    font-family: verdana,arial,sans-serif;\n')
        f.write('    font-size:37px;\n')
        # f.write('    color:#333333;\n')
        f.write('    color:#F00;\n')
        f.write('    border-width: 1px;\n')
        f.write('    border-color: #666666;\n')
        f.write('    border-collapse: collapse;\n')
        f.write('}\n')
        f.write('table.gridtable th {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #666666;\n')
        f.write('    background-color: #dedede;\n')
        f.write('}\n')
        f.write('table.gridtable td {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #666666;\n')
        f.write('    background-color: #ffffff;\n')
        f.write('}\n')
        f.write('/* /gridtable */\n')
            
        f.write('\n')
        f.write('\n')
        f.write('</style>\n')
    
    



        f.write('</head>\n')
        f.write('\n')
        f.write('\n')
        f.write('<body>\n')


        #f.write('<h2  align="center" style="color:blue ; font-size:34px">别人贪婪时我恐惧 别人恐惧时我贪婪</h2>\n')
        f.write('<table class="gridtable">\n')
        
       


        i=0
        column=5
        length=len(images)
        mod_number=length % column
        for add in range(0, column - mod_number):
            # images.append('')
            images.insert(mod_number, '')

        for image in images:
            # '2019-07-10.html' -> '2019-07-10' 
            tmp_image=image[0:image.rfind('.')]
            print("%s" % (tmp_image))
            image = stock_data_dir + '/' + tmp_image + '/' + image


            if i % column == 0:
                f.write('    <tr>\n')

            f.write('        <td>\n')
            f.write('            <a href="%s"  target="_blank"> %s </a>\n' % (image, tmp_image))
            f.write('        </td>\n')
            
            i=i+1

            if i % column  == 0:
                f.write('    </tr>\n')

            f.write('\n')
            
        f.write('</table>\n')
        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')
    
    
    print ('success,images are wrapped up in %s' % (newfile))


def getAllFiles(directory):
    files=[]
    for dirpath, dirnames,filenames in os.walk(directory):
        if filenames!=[]:
            for file in filenames:
                if (file_name + '-index') in file:
                    continue;
                if file_name in file:
                    files.append(dirpath+'/'+file)
    # files.sort(key=len)
    files=sorted(files, reverse=True)
    return files

#获取脚本文件的当前路径
def cur_file_dir():
    #获取脚本路径
    path = sys.path[0]
    #判断为脚本文件还是py2exe编译后的文件，如果是脚本文件，则返回的是脚本的目录，如果是py2exe编译后的文件，则返回的是编译后的文件路径
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)

        
if __name__ == '__main__':
    savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    showImageInHTML(('html'), savedir)#浏览所有jpg,png,gif文件
