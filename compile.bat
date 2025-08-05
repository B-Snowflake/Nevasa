cd .
call .\.venv\Scripts\activate
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
echo %TIME%
nuitka --output-dir=D:\Nevasa_model\Application --enable-plugin=pyside6 --jobs=16 --lto=yes --low-memory --windows-icon-from-ico=resources\icon\Nevasa.ico --standalone --windows-console-mode=disable --include-data-dir=resources/icon=resources/icon  --include-package=pyogrio --include-data-files=.venv/Lib/site-packages/pypinyin/pinyin_dict.json=pypinyin/pinyin_dict.json --include-data-files=.venv/Lib/site-packages/pypinyin/phrases_dict.json=pypinyin/phrases_dict.json  --include-data-dir=.venv/Lib/site-packages/bqplot/map_data=bqplot/map_data --include-data-dir=.venv/Lib/site-packages/rasterio/proj_data=rasterio/proj_data  --include-data-dir=.venv/Lib/site-packages/geemap/data=geemap/data --include-data-dir=.venv/Lib/site-packages/maplibre/srcjs=maplibre/srcjs  --include-data-files=.venv/Lib/site-packages/geemap/examples/datasets.txt=geemap/examples/datasets.txt  --include-data-files=resources/data/data.nev=resources/data/data.nev Nevasa_gee.py
echo %TIME% & ping -n 2 127.0.0.1 > nul
REM --windows-console-mode=disable