SELECT video_id, channel_id, LENGTH(transcript_text) as transcript_len, transcript_available 
FROM `dnd-trends-index.dnd_trends_raw.yt_video_index` 
LIMIT 5;
