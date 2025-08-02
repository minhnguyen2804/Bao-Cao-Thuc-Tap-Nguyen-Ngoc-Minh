CREATE TABLE adnlog_raw_campaign
(
    guid Int64,
    campaign_id Int32,
    click_or_view String,
    event_date Date
)
ENGINE = MergeTree
ORDER BY (event_date, campaign_id, guid);

CREATE TABLE adnlog_raw_banner
(
    guid Int64,
    banner_id Int32,
    click_or_view String,
    event_date Date
)
ENGINE = MergeTree
ORDER BY (event_date, banner_id, guid)