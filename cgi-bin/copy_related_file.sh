target=/var/www/cgi-bin/

cp comm_update.*    $target
cp my_optional.txt  $target
cp hsgt-search.cgi  $target

cp cgi_env.py   $target
cp hello.py     $target


cp ../HData* $target
cp ../comm_generate_web_html.py $target
cp ../get_data_from_db.py $target
cp ../get_daily_zlje.py $target
cp ../file_interface.py $target

cp ../zig.py $target
cp ../plot.py $target
