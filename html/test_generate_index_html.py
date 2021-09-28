#!/usr/bin/env python
#coding:utf-8
import os,sys
import datetime


nowdate=datetime.datetime.now().date()
#nowdate=nowdate-datetime.timedelta(1)
src_dir=nowdate.strftime("%Y-%m-%d")
target_html='index.html'
stock_data_dir="stock_data"

def showImageInHTML(imageTypes,savedir):
    newfile='%s/%s'%(savedir, target_html)
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> index </title>\n')
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
        

        f.write('<h2  align="left" style="color:blue ; font-size:34px">\n')
        f.write('    <tr>\n')

        f.write('<td>\n')
        f.write('     <a href="basic-index.html"  target="_blank"> basic </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="dragon-index.html"  target="_blank"> dragon </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="zlje-index.html"  target="_blank"> zlje </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="hsgt/hsgt-index.html"  target="_blank">hsgt </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="../cgi-bin/hsgt-search.cgi"  target="_blank">hsgt-search </a>\n')
        f.write('</td>\n')

        f.write('    </tr>\n')
        f.write('    <tr>\n')

        f.write('<td>\n')
        f.write('     <a href="macd-index.html"  target="_blank">macd </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="zig-index.html"  target="_blank"> zig </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="quad-index.html"  target="_blank"> quad </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="peach-index.html"  target="_blank"> peach </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="5days-index.html"  target="_blank"> 5days </a>\n')
        f.write('</td>\n')

        f.write('    </tr>\n')
        f.write('    <tr>\n')

        f.write('<td>\n')
        f.write('     <a href="cuptea-index.html"  target="_blank"> cuptea </a>\n')
        f.write('</td>\n')

        f.write('<td>\n')
        f.write('     <a href="duckhead-index.html"  target="_blank"> duckhead </a>\n')
        f.write('</td>\n')
 
        f.write('<td>\n')
        f.write('     <a href="cross3line-index.html"  target="_blank"> cross3line </a>\n')
        f.write('</td>\n')
 
        f.write('<td>\n')
        f.write('     <a href="./stock_data/finance/finance.html"  target="_blank"> finance </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        #f.write('     <a href="./stock_data/holder.html"  target="_blank"> holder </a>\n')
        f.write('     <a href="holder-index.html"  target="_blank"> holder </a>\n')
        f.write('</td>\n')

        f.write('    </tr>\n')
        f.write('    <tr>\n')

        f.write('<td>\n')
        f.write('     <a href="./stock_data/fund.html"  target="_blank"> fund </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="./stock_data/repurchase.html"  target="_blank"> repurchase </a>\n')
        f.write('</td>\n')
        f.write('<td>\n')
        f.write('     <a href="../cgi-bin/comm_update.cgi"  target="_blank">comm_update </a>\n')
        f.write('</td>\n')
        f.write('    </tr>\n')

        f.write('</h2>\n')




        f.write('</table>\n')
        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')
    
    
    print ('success,images are wrapped up in %s' % (newfile))


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
    #savedir= savedir + '/' + stock_data_dir
    showImageInHTML(('html'), savedir)#浏览所有jpg,png,gif文件
