#!/bin/bash
envParam=$1
env=${envParam:="stg"}
rootPath=`pwd`
communityPath=export/communitydb

echo 'Step #1. Push to Elasticsearch'
esHostParam=$2
esHost=${esHostParam:="http://localhost:9200"}
articleIndex=community-stg
articleType=articles
userIndex=user-stg
if [ $env  == "prd" ]; then
    articleIndex=community
    userIndex=user
fi

echo "==================================================================================="
echo "Step #2. Articles"
echo " >>> esHost: $esHost"
echo " >>> indexName: $articleIndex"

echo "Step #2-1. DELETE INDEX $articleIndex"
python esLoader.py \
        --es-host $esHost \
        --index $articleIndex \
        --type $articleType \
        --delete

settingsFilePath=config/mappings_community.json
articlesPath=$rootPath/$communityPath/articles.parquet
commentsPath=$rootPath/$communityPath/comments.parquet

echo "Step #2-2. INSERT $articlesPath"
python esLoader.py \
        --es-host $esHost \
        --index $articleIndex \
        --type $articleType \
        --index_settings_file $settingsFilePath \
        --id_field ID \
        parquet $articlesPath

echo "Step #2-2. INSERT $commentsPath"
python esLoader.py \
        --es-host $esHost \
        --index $articleIndex \
        --type $articleType \
        --id_field ID \
        --as_child \
        --parent_id_field ARTICLES_ID \
        parquet $commentsPath 

echo "Step #2-3. Finished"
echo "==================================================================================="

echo "==================================================================================="
echo "Step #3. Users"
echo " >>> esHost: $esHost"
echo " >>> indexName: $userIndex"

echo "Step #3-1. DELETE INDEX $userIndex"
python esLoader.py \
        --es-host $esHost \
        --index $userIndex \
        --delete

settingsFilePath=config/mappings_user.json
usersPath=$rootPath/$communityPath/users.csv

echo "Step #3-2. INSERT $usersPath"
python esLoader.py \
        --es-host $esHost \
        --index $userIndex \
        --index_settings_file $settingsFilePath \
        --id_field user_id \
        csv $usersPath

echo "Step #3-3. Finished"
echo "==================================================================================="
