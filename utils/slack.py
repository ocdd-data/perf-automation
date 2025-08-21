import os
from logging import Logger

import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackBot:
  def __init__(self):
    self.client = WebClient(token=os.getenv('SLACK_TOKEN'))
    self.logger = Logger('SlackBot')

  def uploadFile(self, file: str, channel: str, comment: str) -> None:
    filename = os.path.basename(file)
    filesize = os.path.getsize(file)

    try:
      ticket = self.client.files_getUploadURLExternal(
        filename=filename,
        length=filesize,
      )
      upload_url = ticket["upload_url"]
      file_id = ticket["file_id"]

      with open(file, "rb") as f:
        resp = requests.post(upload_url, files={"file": (filename, f)})
        resp.raise_for_status()

      result = self.client.files_completeUploadExternal(
        files=[{"id": file_id, "title": filename}],
        channel_id=channel,
        initial_comment=comment or "",
      )
      self.logger.info(result)

    except requests.RequestException as e:
      self.logger.error(f"HTTP error during file upload: {e}")
      try:
        self.client.chat_postMessage(
          channel=channel,
          text=f":warning: Error uploading *{filename}*: {e}"
        )
      except SlackApiError as e2:
        self.logger.error(f"Also failed to notify channel: {e2}")

    except SlackApiError as e:
      self.logger.error(f"Slack API error: {e}")
      try:
        self.client.chat_postMessage(
          channel=channel,
          text=f":warning: Slack API error uploading *{filename}*: {e}"
        )
      except SlackApiError as e2:
        self.logger.error(f"Also failed to notify channel: {e2}")
