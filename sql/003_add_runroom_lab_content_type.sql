ALTER TABLE content_items
DROP CONSTRAINT IF EXISTS content_items_content_type_check;

ALTER TABLE content_items
ADD CONSTRAINT content_items_content_type_check
CHECK (content_type IN ('episode', 'case_study', 'runroom_lab', 'article', 'event', 'training', 'other'));
