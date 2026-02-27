from __future__ import annotations

import json
from pathlib import Path


def load_openapi_spec(web_dir: Path) -> dict:
    auto_path = web_dir / "openapi.auto.json"
    if auto_path.exists() and auto_path.is_file():
        try:
            loaded = json.loads(auto_path.read_text("utf-8"))
            if isinstance(loaded, dict) and str(loaded.get("openapi") or "").strip():
                return loaded
        except Exception:
            pass

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "YouTube Direct Bot Web API",
            "version": "1.0.0",
            "description": "API for transcript saving, analysis, transcript Q&A, and public research browsing.",
        },
        "paths": {
            "/api/videos": {
                "get": {
                    "summary": "List videos",
                    "responses": {
                        "200": {
                            "description": "Video list",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/VideosResponse"}
                                }
                            },
                        }
                    },
                }
            },
            "/api/video": {
                "get": {
                    "summary": "Get video detail",
                    "parameters": [
                        {
                            "name": "video_id",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Video detail",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/VideoDetailResponse"}
                                }
                            },
                        },
                        "400": {
                            "description": "Missing video_id",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                    },
                }
            },
            "/api/researches": {
                "get": {
                    "summary": "List public research runs",
                    "responses": {
                        "200": {
                            "description": "Public researches",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ResearchesResponse"}
                                }
                            },
                        }
                    },
                }
            },
            "/api/research": {
                "get": {
                    "summary": "Get one public research run",
                    "parameters": [
                        {
                            "name": "run_id",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Research detail",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ResearchDetailResponse"}
                                }
                            },
                        },
                        "400": {
                            "description": "Missing run_id",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                        "404": {
                            "description": "Research not found",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                    },
                }
            },
            "/api/save_transcript": {
                "post": {
                    "summary": "Save transcript by URL",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/SaveTranscriptRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Transcript saved",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/SaveTranscriptResponse"}
                                }
                            },
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                    },
                }
            },
            "/api/analyze": {
                "post": {
                    "summary": "Run AI analysis for one video",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/AnalyzeRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Analysis result",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/AnalyzeResponse"}
                                }
                            },
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                    },
                }
            },
            "/api/ask": {
                "post": {
                    "summary": "Ask transcript Q&A",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/AskRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Q&A response",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/AskResponse"}
                                }
                            },
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                                }
                            },
                        },
                    },
                }
            },
            "/api/clear_history": {
                "post": {
                    "summary": "Clear saved index/transcript history",
                    "requestBody": {
                        "required": False,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/ClearHistoryRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "History cleared",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ClearHistoryResponse"}
                                }
                            },
                        }
                    },
                }
            },
            "/api/openapi.json": {
                "get": {
                    "summary": "OpenAPI specification",
                    "responses": {
                        "200": {
                            "description": "OpenAPI JSON",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        }
                    },
                }
            },
        },
        "components": {
            "schemas": {
                "ErrorResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean", "example": False},
                        "error": {"type": "string"},
                    },
                    "required": ["ok", "error"],
                },
                "VideosResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "items": {"type": "array", "items": {"type": "object"}},
                    },
                    "required": ["ok", "items"],
                },
                "VideoDetailResponse": {
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}, "item": {"type": "object"}},
                    "required": ["ok", "item"],
                },
                "ResearchesResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "items": {"type": "array", "items": {"type": "object"}},
                    },
                    "required": ["ok", "items"],
                },
                "ResearchDetailResponse": {
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}, "item": {"type": "object"}},
                    "required": ["ok", "item"],
                },
                "SaveTranscriptRequest": {
                    "type": "object",
                    "properties": {"url": {"type": "string"}, "force": {"type": "boolean"}},
                    "required": ["url"],
                },
                "SaveTranscriptResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "elapsed_sec": {"type": "number"},
                        "video_id": {"type": "string"},
                        "title": {"type": "string"},
                        "transcript_path": {"type": "string"},
                        "source": {"type": "string"},
                        "cached": {"type": "boolean"},
                    },
                    "required": ["ok", "elapsed_sec", "video_id", "title", "transcript_path", "source", "cached"],
                },
                "AnalyzeRequest": {
                    "type": "object",
                    "properties": {
                        "video_id": {"type": "string"},
                        "force": {"type": "boolean"},
                        "save": {"type": "boolean"},
                    },
                    "required": ["video_id"],
                },
                "AnalyzeResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "video_id": {"type": "string"},
                        "elapsed_sec": {"type": "number"},
                        "analysis": {"type": "string"},
                        "cached": {"type": "boolean"},
                        "cache_age_sec": {"type": "number"},
                        "lang": {"type": "string"},
                    },
                    "required": ["ok", "video_id", "elapsed_sec", "analysis", "cached", "cache_age_sec", "lang"],
                },
                "AskRequest": {
                    "type": "object",
                    "properties": {"video_id": {"type": "string"}, "question": {"type": "string"}},
                    "required": ["video_id", "question"],
                },
                "AskResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "video_id": {"type": "string"},
                        "elapsed_sec": {"type": "number"},
                        "answer": {"type": "string"},
                    },
                    "required": ["ok", "video_id", "elapsed_sec", "answer"],
                },
                "ClearHistoryRequest": {
                    "type": "object",
                    "properties": {"delete_files": {"type": "boolean"}},
                },
                "ClearHistoryResponse": {
                    "type": "object",
                    "properties": {
                        "ok": {"type": "boolean"},
                        "elapsed_sec": {"type": "number"},
                        "removed_index_entries": {"type": "integer"},
                        "removed_transcripts": {"type": "integer"},
                        "removed_captions": {"type": "integer"},
                    },
                    "required": [
                        "ok",
                        "elapsed_sec",
                        "removed_index_entries",
                        "removed_transcripts",
                        "removed_captions",
                    ],
                },
            }
        },
    }
