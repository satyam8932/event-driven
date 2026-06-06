import enum


class JobStatus(enum.StrEnum):
    PENDING = "PENDING"
    PARSING = "PARSING"
    TTS = "TTS"
    STITCHING = "STITCHING"
    NOTIFYING = "NOTIFYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskStage(enum.StrEnum):
    PARSE = "parse"
    TTS = "tts"
    STITCH = "stitch"
    NOTIFY = "notify"


class TaskStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    FAILED = "FAILED"
    DEAD = "DEAD"


# Allowed job status transitions per stage completion
JOB_STAGE_TRANSITIONS: dict[TaskStage, tuple[JobStatus, JobStatus]] = {
    TaskStage.PARSE: (JobStatus.PENDING, JobStatus.PARSING),
    TaskStage.TTS: (JobStatus.PARSING, JobStatus.TTS),
    TaskStage.STITCH: (JobStatus.TTS, JobStatus.STITCHING),
    TaskStage.NOTIFY: (JobStatus.STITCHING, JobStatus.NOTIFYING),
}

ROUTING_KEY: dict[TaskStage, str] = {
    TaskStage.PARSE: "job.parse",
    TaskStage.TTS: "job.tts",
    TaskStage.STITCH: "job.stitch",
    TaskStage.NOTIFY: "job.notify",
}
