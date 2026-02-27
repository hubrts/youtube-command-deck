#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path


def _extract_paths_for_method(tree: ast.AST, method_name: str) -> set[str]:
    paths: set[str] = set()
    app_handler: ast.ClassDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "AppHandler":
            app_handler = node
            break
    if app_handler is None:
        return paths

    method_node: ast.FunctionDef | None = None
    for node in app_handler.body:
        if isinstance(node, ast.FunctionDef) and node.name == method_name:
            method_node = node
            break
    if method_node is None:
        return paths

    for node in ast.walk(method_node):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not isinstance(test, ast.Compare):
            continue
        if not isinstance(test.left, ast.Name) or test.left.id != "path":
            continue
        if len(test.ops) != 1 or len(test.comparators) != 1:
            continue

        op = test.ops[0]
        rhs = test.comparators[0]
        if isinstance(op, ast.Eq) and isinstance(rhs, ast.Constant) and isinstance(rhs.value, str):
            paths.add(rhs.value)
            continue
        if isinstance(op, ast.In) and isinstance(rhs, (ast.Tuple, ast.List)):
            for elt in rhs.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    paths.add(elt.value)

    return paths


def _clean_paths(paths: set[str]) -> list[str]:
    keep: list[str] = []
    for path in sorted(paths):
        if path.startswith("/api/"):
            keep.append(path)
            continue
        if path in ("/openapi.json", "/swagger", "/swagger/"):
            keep.append(path)
    return keep


def _op_summary(path: str, method: str) -> str:
    known: dict[tuple[str, str], str] = {
        ("/api/videos", "get"): "List videos",
        ("/api/video", "get"): "Get video detail",
        ("/api/runtime", "get"): "Get web runtime configuration",
        ("/api/researches", "get"): "List public researches",
        ("/api/research", "get"): "Get one public research",
        ("/api/save_transcript", "post"): "Save transcript from URL",
        ("/api/analyze", "post"): "Run transcript analysis",
        ("/api/ask", "post"): "Ask transcript",
        ("/api/clear_history", "post"): "Clear saved history",
        ("/api/direct_video", "post"): "Get direct video download URL",
        ("/api/direct_audio", "post"): "Get direct audio download URL",
        ("/api/knowledge_juice", "post"): "Run Knowledge Juice Maker",
        ("/api/knowledge_juice", "get"): "Get one Knowledge Juice report",
        ("/api/knowledge_juices", "get"): "List Knowledge Juice reports",
        ("/api/knowledge_juice/jobs", "get"): "List Knowledge Juice jobs",
        ("/api/knowledge_juice/job", "get"): "Get one Knowledge Juice job",
        ("/api/knowledge_juice/start", "post"): "Start Knowledge Juice job",
        ("/api/openapi.json", "get"): "OpenAPI document",
        ("/openapi.json", "get"): "OpenAPI document",
        ("/swagger", "get"): "Swagger UI page",
        ("/swagger/", "get"): "Swagger UI page",
    }
    fallback = f"{method.upper()} {path}"
    return known.get((path, method), fallback)


def _query_params(path: str, method: str) -> list[dict]:
    if method != "get":
        return []
    if path == "/api/video":
        return [{"name": "video_id", "in": "query", "required": True, "schema": {"type": "string"}}]
    if path == "/api/research":
        return [{"name": "run_id", "in": "query", "required": True, "schema": {"type": "string"}}]
    if path == "/api/knowledge_juice":
        return [{"name": "run_id", "in": "query", "required": True, "schema": {"type": "string"}}]
    if path == "/api/knowledge_juice/job":
        return [{"name": "job_id", "in": "query", "required": True, "schema": {"type": "string"}}]
    if path == "/api/knowledge_juice/jobs":
        return [{"name": "active_only", "in": "query", "required": False, "schema": {"type": "string"}}]
    return []


def _request_schema(path: str) -> dict | None:
    if path == "/api/save_transcript":
        return {
            "type": "object",
            "properties": {"url": {"type": "string"}, "force": {"type": "boolean"}},
            "required": ["url"],
        }
    if path == "/api/analyze":
        return {
            "type": "object",
            "properties": {
                "video_id": {"type": "string"},
                "force": {"type": "boolean"},
                "save": {"type": "boolean"},
            },
            "required": ["video_id"],
        }
    if path == "/api/ask":
        return {
            "type": "object",
            "properties": {"video_id": {"type": "string"}, "question": {"type": "string"}},
            "required": ["video_id", "question"],
        }
    if path == "/api/clear_history":
        return {
            "type": "object",
            "properties": {"delete_files": {"type": "boolean"}},
        }
    if path == "/api/knowledge_juice":
        return {
            "type": "object",
            "properties": {"topic": {"type": "string"}, "private_run": {"type": "boolean"}},
            "required": ["topic"],
        }
    if path == "/api/knowledge_juice/start":
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "private_run": {"type": "boolean"},
                "max_videos": {"type": "integer"},
                "max_queries": {"type": "integer"},
                "per_query": {"type": "integer"},
                "min_duration_sec": {"type": "integer"},
                "max_duration_sec": {"type": "integer"},
            },
            "required": ["topic"],
        }
    if path in ("/api/direct_video", "/api/direct_audio"):
        return {
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        }
    return None


def _success_content(path: str) -> dict:
    if path in ("/swagger", "/swagger/"):
        return {"text/html": {"schema": {"type": "string"}}}
    return {"application/json": {"schema": {"type": "object"}}}


def build_openapi_from_web_app(web_app_path: Path) -> dict:
    source = web_app_path.read_text("utf-8")
    tree = ast.parse(source)

    get_paths = _clean_paths(_extract_paths_for_method(tree, "do_GET"))
    post_paths = _clean_paths(_extract_paths_for_method(tree, "do_POST"))

    merged: dict[str, set[str]] = {}
    for p in get_paths:
        merged.setdefault(p, set()).add("get")
    for p in post_paths:
        merged.setdefault(p, set()).add("post")

    paths: dict[str, dict] = {}
    for path in sorted(merged.keys()):
        path_item: dict[str, dict] = {}
        for method in sorted(merged[path]):
            op: dict = {
                "summary": _op_summary(path, method),
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": _success_content(path),
                    }
                },
            }
            params = _query_params(path, method)
            if params:
                op["parameters"] = params
            req_schema = _request_schema(path) if method == "post" else None
            if req_schema is not None:
                op["requestBody"] = {
                    "required": True,
                    "content": {"application/json": {"schema": req_schema}},
                }
                op["responses"]["400"] = {
                    "description": "Bad request",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"ok": {"type": "boolean"}, "error": {"type": "string"}},
                                "required": ["ok", "error"],
                            }
                        }
                    },
                }
            path_item[method] = op
        paths[path] = path_item

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "YouTube Direct Bot Web API",
            "version": "1.0.0",
            "description": "Auto-generated from AppHandler routes in web_app.py",
        },
        "paths": paths,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-generate OpenAPI JSON from web_app.py routes")
    parser.add_argument(
        "--web-app",
        default="/home/illia/youtube_direct_bot/web_app.py",
        help="Path to web_app.py",
    )
    parser.add_argument(
        "--output",
        default="/home/illia/youtube_direct_bot/web/openapi.auto.json",
        help="Path to output JSON",
    )
    args = parser.parse_args()

    web_app_path = Path(args.web_app).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    spec = build_openapi_from_web_app(web_app_path)
    output_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Generated: {output_path}")
    print(f"Paths: {len(spec.get('paths') or {})}")


if __name__ == "__main__":
    main()
