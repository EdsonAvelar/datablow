

git init
git remote add origin https://github.com/EdsonAvelar/datablow.git
git fetch origin
git checkout origin/master -- _site
cp -r _site/* .


