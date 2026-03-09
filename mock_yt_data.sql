-- Mocking a video for Phase Y2 Integration Test
INSERT INTO `dnd-trends-index.dnd_trends_raw.yt_video_index` (
    video_id, 
    channel_id, 
    title, 
    published_at, 
    transcript_text, 
    transcript_available, 
    ingestion_date
)
VALUES (
    'MOCK_VIDEO_V2_001',
    'UCi-PqisPTpljX0TUN0N_7gA',
    'The 2024 Ranger is BROKEN (Oracle v2 Test)',
    CURRENT_TIMESTAMP(),
    'The new Ranger is absolutely broken. It is stupid good. But honestly, Reddit thinks the design is half-baked and lazy. I disagree, I think it is criminal how strong the Weapon Mastery is. It is just nasty.',
    TRUE,
    CURRENT_DATE()
);
