-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE public.media_speaker_mapping
ADD COLUMN ARTIFACT_NAME VARCHAR

-- depends: 0001.create-public.media_speaker_mapping
ALTER TABLE public.media_speaker_mapping
DROP COLUMN IF EXISTS ARTIFACT_NAME
