# -*- coding: utf-8 -*-
#
#     Author : Suphakit Annoppornchai
#     Date   : Apr 5 2017
#     Name   : bot
#
#          https://saixiii.ddns.net
# 
# Copyright (C) 2017  Suphakit Annoppornchai
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.

from __future__ import unicode_literals

import sys
import os
import json
import errno
import time
import tempfile
import logging
from logging.handlers import RotatingFileHandler
from logging import Formatter
from time import strftime
from argparse import ArgumentParser
from kafka import KafkaProducer
from kafka.errors import KafkaError

from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    SourceUser, SourceGroup, SourceRoom,
    TemplateSendMessage, ConfirmTemplate, MessageTemplateAction,
    ButtonsTemplate, URITemplateAction, PostbackTemplateAction,
    CarouselTemplate, CarouselColumn, PostbackEvent,
    StickerMessage, StickerSendMessage, LocationMessage, LocationSendMessage,
    ImageMessage, VideoMessage, AudioMessage,
    UnfollowEvent, FollowEvent, JoinEvent, LeaveEvent, BeaconEvent
)

app = Flask(__name__)

#-------------------------------------------------------------------------------
#     G L O B A L    V A R I A B L E S
#-------------------------------------------------------------------------------

botname = 'Saixiii'
botcall = '-'
botlen  = len(botcall)

# get channel_secret and channel_access_token from your environment variable
channel_secret = 'a4830b56ff9e5d85c3ff4a67f2360ee1'
channel_access_token = 'jibJtKouOP8/0UYtTRtlXcB70zzJlfKtBnCXH7m4OwgsEwRKezyI8/E8frwhWSUNtJ5efliVvp4eOCFyNksfrGCXAcsvVq7O8idF7dy1fesLrsrN0Nm4RGR1vkYxGSphrwhAHSFlm9kX7FYZmkxFTwdB04t89/1O/w1cDnyilFU='

# Log file configuration
static_tmp_path = os.path.join(os.path.dirname(__file__), 'static', 'content')
chat_file = os.path.join(os.path.dirname(__file__), 'log', botname) + '.msg'
log_file = os.path.join(os.path.dirname(__file__), 'log', botname) + '.log'
log_size = 1024 * 1024 * 10
log_backup = 50
log_format = '[%(asctime)s] [%(levelname)s] - %(message)s'
log_mode = logging.DEBUG

# Kafka configuration
kafka_topic = 'line-saixiii'
kafka_ip = 'saixiii.ddns.net'
kafka_port = '9092'

#-------------------------------------------------------------------------------
#     I N I T I A L    P R O G R A M
#-------------------------------------------------------------------------------

if channel_secret is None:
    print('Specify LINE_CHANNEL_SECRET as environment variable.')
    sys.exit(1)
if channel_access_token is None:
    print('Specify LINE_CHANNEL_ACCESS_TOKEN as environment variable.')
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

#-------------------------------------------------------------------------------
#     F U N C T I O N S
#-------------------------------------------------------------------------------

# convert epoch date to date time
def convert_epoch(epoch):
  sec = float(epoch) / 1000
  dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sec))
  return str(dt)

# Function check call bot
def chkcall(msg):
	if msg != None and msg[:botlen].lower() == botcall:
		return True
	else:
		return False

# function for create tmp dir for download content
def make_static_tmp_dir():
    try:
        os.makedirs(static_tmp_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(static_tmp_path):
            pass
        else:
            raise
            
# Kafka - fuction producer create messages to topic
def kafka_producer(event):
    # create kafka producer instance
    producer = KafkaProducer(bootstrap_servers=[kafka_ip + ':' + kafka_port],value_serializer=lambda v: json.dumps(v).encode('utf-8'),retries=5)
    
    # get MessageEvent data
    msg_dict = dict()
    msg_dict['replytoken'] = event.reply_token
    msg_dict['timestamp'] = event.timestamp
    msg_dict['datetime'] = convert_epoch(event.timestamp)
    msg_dict['source'] = event.source.type
    msg_dict['msg'] = event.message.type
    
    if isinstance(event.message, TextMessage):
      msg_dict['content'] = event.message.text
    
    if isinstance(event.source, SourceUser):
      profile = line_bot_api.get_profile(event.source.user_id)
      msg_dict['id'] = profile.user_id
      msg_dict['name'] = profile.display_name
      app.logger.info('User - ' + msg_dict['name'] + ' : ' + msg_dict['content'])
    elif isinstance(event.source, SourceGroup):
    	msg_dict['id'] = event.source.group_id
    	app.logger.info('Group - ' + msg_dict['id'] + ' : ' + msg_dict['content'])
    elif isinstance(event.source, SourceRoom):
    	msg_dict['id'] = event.source.room_id
    	app.logger.info('Room - ' + msg_dict['id'] + ' : ' + msg_dict['content'])
    
    # convert dict to json
    msg_json = json.dumps(msg_dict)
    
    # push message asynchronous
    producer.send(kafka_topic, msg_json)
    producer.flush()
    #app.logger.debug(msg_json.decode('utf-8'))


@app.before_first_request
def setup_logging():
    # add log handler
    loghandler = RotatingFileHandler(log_file, maxBytes=log_size, backupCount=log_backup)
    loghandler.setFormatter(Formatter(log_format))
    loghandler.setLevel(log_mode)

    app.logger.addHandler(loghandler)
    app.logger.setLevel(log_mode)
    
    
    
@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.debug("Request body: " + body)
    
    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text
    kafka_producer(event)
    
    if text == 'profile':
        if isinstance(event.source, SourceUser):
            profile = line_bot_api.get_profile(event.source.user_id)
            line_bot_api.reply_message(
                event.reply_token, [
                    TextSendMessage(
                        text='Display name: ' + profile.display_name
                    ),
                    TextSendMessage(
                        text='Status message: ' + profile.status_message
                    )
                ]
            )
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextMessage(text="Bot can't use profile API without user ID"))
    elif text == 'bye':
        if isinstance(event.source, SourceGroup):
            line_bot_api.reply_message(
                event.reply_token, TextMessage(text='Leaving group'))
            line_bot_api.leave_group(event.source.group_id)
        elif isinstance(event.source, SourceRoom):
            line_bot_api.reply_message(
                event.reply_token, TextMessage(text='Leaving group'))
            line_bot_api.leave_room(event.source.room_id)
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextMessage(text="Bot can't leave from 1:1 chat"))
    elif text == 'confirm':
        confirm_template = ConfirmTemplate(text='Do it?', actions=[
            MessageTemplateAction(label='Yes', text='Yes!'),
            MessageTemplateAction(label='No', text='No!'),
        ])
        template_message = TemplateSendMessage(
            alt_text='Confirm alt text', template=confirm_template)
        line_bot_api.reply_message(event.reply_token, template_message)
    elif text == 'buttons':
        buttons_template = ButtonsTemplate(
            title='My buttons sample', text='Hello, my buttons', actions=[
                URITemplateAction(
                    label='Go to line.me', uri='https://line.me'),
                PostbackTemplateAction(label='ping', data='ping'),
                PostbackTemplateAction(
                    label='ping with text', data='ping',
                    text='ping'),
                MessageTemplateAction(label='Translate Rice', text='米')
            ])
        template_message = TemplateSendMessage(
            alt_text='Buttons alt text', template=buttons_template)
        line_bot_api.reply_message(event.reply_token, template_message)
    elif text == 'carousel':
        carousel_template = CarouselTemplate(columns=[
            CarouselColumn(text='hoge1', title='fuga1', actions=[
                URITemplateAction(
                    label='Go to line.me', uri='https://line.me'),
                PostbackTemplateAction(label='ping', data='ping')
            ]),
            CarouselColumn(text='hoge2', title='fuga2', actions=[
                PostbackTemplateAction(
                    label='ping with text', data='ping',
                    text='ping'),
                MessageTemplateAction(label='Translate Rice', text='米')
            ]),
        ])
        template_message = TemplateSendMessage(
            alt_text='Buttons alt text', template=carousel_template)
        line_bot_api.reply_message(event.reply_token, template_message)
    elif text == 'imagemap':
        pass
    elif text[:1].lower() == '-':
        line_bot_api.reply_message(
            event.reply_token, TextSendMessage(text=event.message.text))


@handler.add(MessageEvent, message=LocationMessage)
def handle_location_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        LocationSendMessage(
            title=event.message.title, address=event.message.address,
            latitude=event.message.latitude, longitude=event.message.longitude
        )
    )


@handler.add(MessageEvent, message=StickerMessage)
def handle_sticker_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        StickerSendMessage(
            package_id=event.message.package_id,
            sticker_id=event.message.sticker_id)
    )


# Other Message Type
@handler.add(MessageEvent, message=(ImageMessage, VideoMessage, AudioMessage))
def handle_content_message(event):
    if isinstance(event.message, ImageMessage):
        ext = 'jpg'
    elif isinstance(event.message, VideoMessage):
        ext = 'mp4'
    elif isinstance(event.message, AudioMessage):
        ext = 'm4a'
    else:
        return

    message_content = line_bot_api.get_message_content(event.message.id)
    with tempfile.NamedTemporaryFile(dir=static_tmp_path, prefix=ext + '-', delete=False) as tf:
        for chunk in message_content.iter_content():
            tf.write(chunk)
        tempfile_path = tf.name

    dist_path = tempfile_path + '.' + ext
    dist_name = os.path.basename(dist_path)
    os.rename(tempfile_path, dist_path)

    line_bot_api.reply_message(
        event.reply_token, [
            TextSendMessage(text='Save content.'),
            TextSendMessage(text=request.host_url + os.path.join('static', 'content', dist_name))
        ])


@handler.add(FollowEvent)
def handle_follow(event):
    line_bot_api.reply_message(
        event.reply_token, TextSendMessage(text='Got follow event'))


@handler.add(UnfollowEvent)
def handle_unfollow():
    app.logger.info("Got Unfollow event")


@handler.add(JoinEvent)
def handle_join(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text='Joined this ' + event.source.type))


@handler.add(LeaveEvent)
def handle_leave():
    app.logger.info("Got leave event")


@handler.add(PostbackEvent)
def handle_postback(event):
    if event.postback.data == 'ping':
        line_bot_api.reply_message(
            event.reply_token, TextSendMessage(text='pong'))


@handler.add(BeaconEvent)
def handle_beacon(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text='Got beacon event. hwid=' + event.beacon.hwid))


if __name__ == "__main__":
    arg_parser = ArgumentParser(
        usage='Usage: python ' + __file__ + ' [--port <port>] [--help]'
    )
    arg_parser.add_argument('-p', '--port', default=8000, help='port')
    arg_parser.add_argument('-d', '--debug', default=False, help='debug')
    options = arg_parser.parse_args()

    # create tmp dir for download content
    make_static_tmp_dir()
    
    app.run(debug=options.debug, port=options.port)