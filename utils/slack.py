import os
from logging import Logger

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackBot:
  def __init__(self):
    self.client = WebClient(token=os.getenv('SLACK_TOKEN'))
    self.logger = Logger('SlackBot')

  def uploadFile(self, file:str, channel:str, comment:str) -> None:
    try:
      result = self.client.files_upload_v2(
        channel=channel,
        initial_comment=comment,
        file=file,
      )
      self.logger.info(result)
    except SlackApiError as e:
      try:
        self.client.chat_postMessage(
          channel=channel,
          text=f"Error uploading file: {e}"
        )
      except SlackApiError as e:
        self.logger.error("Error uploading file: {}".format(e))
