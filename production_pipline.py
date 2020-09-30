import argparse
import csv
import datetime
import json
import os
import pandas as pd
import re
import requests
import uuid

from datetime import timedelta
from google.cloud import storage
from google.cloud import bigquery


# 抽出・変換
def call_slack_api(target):
    # 設定
    token = "あなたのトークン"
    channles_id = "https://slack.com/api/channels.list"
    users=  "https://slack.com/api/users.list"
    talk_history = "https://slack.com/api/channels.history?count=1000&token=" + token + "&channel="
    target_date = target.strftime("%Y-%m-%d")
    oldest = target
    latest = latest + datetime.timedelta(days=1)
    payload = {
        "token":token
    }

    # チャンネル情報の抽出・変換
    channel_response = requests.get(channles_id, params=payload)
    channel_json = channel_response.json()
    channel_df = pd.DataFrame(data=channel_json['channels'])
    channel_df = channel_df[channel_df['is_archived'] == False ]
    channel_df = channel_df[['id', 'name']]
    channel_df["target_date"] = target_date

    # ユーザー情報の抽出・変換
    user_response = requests.get(users, params=payload)
    users_json = user_response.json()
    users_df = pd.DataFrame(data=users_json['members'])
    users_cols = ['user_id', 'real_name_normalized', 'display_name_normalized', 'target_date']
    profiles_df = pd.DataFrame(columns=users_cols)
    for index, row in users_df.iterrows():
        user_id = row['id']
        if row['deleted'] == False:
            profile = row['profile']
            user_profile = pd.DataFrame({
                'user_id':[user_id],
                'real_name_normalized':[profile['real_name_normalized']],
                'display_name_normalized':[profile['display_name_normalized']],
                'target_date': target_date
            })
            profiles_df = profiles_df.append(user_profile)

    # 全てのトーク履歴を抽出
    all_talks_df = pd.DataFrame()
    for index, channel in channel_df.iterrows():
        channel_id = channel['id']
        response_talk = requests.get(talk_history + channel_id + "&oldest=" + str(oldest.timestamp()) + "&latest=" + str(latest.timestamp()))
        talk_json = response_talk.json()
        if 'messages' in talk_json:
            channel_talk_df = pd.DataFrame(data=talk_json['messages'])
        channel_talk_df['channel_id'] = channel_id
        all_talks_df = all_talks_df.append(channel_talk_df)


    # トーク履歴、リアクション、メンション情報の変換
    talk_cols = ['channel_id', 'talk_id', 'ts', 'thread_ts', 'talk_user', 'text','target_date']
    reaction_cols = ['channel_id', 'talk_id', 'talk_user', 'reaction_user', 'emoji','target_date']
    mention_cols = ['channel_id', 'talk_id', 'talk_user', 'mention_user','target_date']
    talks_all = pd.DataFrame(columns=talk_cols)
    talk_reactions_all = pd.DataFrame(columns=reaction_cols)
    talk_mentions_all = pd.DataFrame(columns=mention_cols)


    for index, row in all_talks_df.iterrows():
        talk_id = str(uuid.uuid4())

        # トーク履歴
        try:
            talk = pd.DataFrame({
                'channel_id':[row['channel_id']], 
                'talk_id':[talk_id],
                'ts':[row['ts']],
                'thread_ts':[row['thread_ts']],
                'talk_user':[row['user']],
                'text':[row['text']],
                'reply_count':[row['reply_count']],
                'reply_users_count':[row['reply_users_count']],
                'target_date': target_date
                })
            talks_all = talks_all.append(talk)
        except KeyError:
            talk = pd.DataFrame({
                'channel_id':[row['channel_id']], 
                'talk_id':[talk_id],
                'ts':[row['ts']],
                'talk_user':[row['user']],
                'text':[row['text']],
                'target_date': target_date
                })
            talks_all = talks_all.append(talk)

        # リアクション
        try:
            if row['reactions'] == row['reactions']:
                reactions = row['reactions']
                for reaction in reactions:
                    emoji = reaction['name']
                    reaction_users = reaction['users']
                    for reaction_user in reaction_users:
                        talk_reaction = pd.DataFrame({
                                'channel_id' : [row['channel_id']],
                                'talk_id':[talk_id],
                                'talk_user':[row['user']],
                                'reaction_user': [reaction_user],
                                'emoji':[emoji],
                                'target_date': target_date
                                })
                        talk_reactions_all = talk_reactions_all.append(talk_reaction)
        except KeyError:
            talk_reactions_all = talk_reactions_all

        # メンション
        mentions = re.findall('<@[0-9a-zA-Z_./?-]{9}>', row['text'])
        for mention in mentions:
            mention_user = mention[2:-1]
            talk_mention = pd.DataFrame({
                    'channel_id' : [row['channel_id']],
                    'talk_id':[talk_id],
                    'talk_user':[row['user']],
                    'mention_user':[mention_user],
                    'target_date': target_date
                    })
            talk_mentions_all = talk_mentions_all.append(talk_mention)

    channels = channel_df.to_csv
    users = profiles_df.to_csv
    talks = talks_all.to_csv
    reactions = talk_reactions_all.to_csv
    mentions = talk_mentions_all.to_csv

    return channels, users, talks, reactions, mentions 


def upload_to_gcs(target_date, table_name):

    target_date_str = str(target_date)
    client = storage.Client()
    bucket = client.get_bucket('バケット名')
    fname = '{table_name}/{table_name}_{target_date}.csv'.format(table_name=table_name, target_date=target_date)
    blob_channels = bucket.blob(fname)
    blob_channels.upload_from_filename(filename='/home/slack/csv/{table_name}.csv'.format(table_name=table_name))


def load_bq(target_date, table_name):

    client = bigquery.Client()
    dataset_ref = client.dataset('データセット名')
    table_id = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.allow_quoted_newlines = True

    uri = "gs://バケット名/{table_name}/{table_name}_{target_date}.csv".format(target_date=target_date, table_name=table_name)
    load_job=client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()


def main(target_date):

    # 抽出・変換データを取得
    target_datetime = datetime.datetime.strptime(target_date, '%Y%m%d')
    channels, users, talks, reactions, mentions = call_slack_api(target_datetime)

    # ローカルファイルに書き込み
    channels("/home/slack/csv/channel.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    users("/home/slack/csv/user.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    talks("/home/slack/csv/talk.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    reactions("/home/slack/csv/reaction.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    mentions("/home/slack/csv/mention.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    # GCSに取込
    upload_to_gcs(target_date, 'channel')
    upload_to_gcs(target_date, 'user')
    upload_to_gcs(target_date, 'talk')
    upload_to_gcs(target_date, 'reaction')
    upload_to_gcs(target_date, 'mention')

    # BigQueryに取込
    load_bq(target_date, 'channel')
    load_bq(target_date, 'user')
    load_bq(target_date, 'talk')
    load_bq(target_date, 'reaction')
    load_bq(target_date, 'mention')


if __name__ == '__main__':

    # コマンドライン引数の設定
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='target_date', type=str, required=False)
    # コマンドへの引数の取得
    args = parser.parse_args()
    target_date = args.target_date
    if target_date is None:
        target_date = (datetime.datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    main(target_date)
