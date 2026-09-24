#!/usr/bin/env python3
"""Generate the model-facing reference for mcp-hub-client as AsciiDoc.

Everything here is taken from a running client over stdio, not copied from
source, so it is exactly what a model is shown: the tool list and every
description and parameter in both delivery modes, the extra hub_connect
note MCP_HUB_PROJECT_DIR adds, and an example hub_connect result against a
local mcp-hub-server. The two startup notices are quoted from
internal/mcptools/startupnotice.go, since they need a previous run's state
to appear.

    scripts/tool-reference.py                    # print the fragment
    scripts/tool-reference.py --update FILE.adoc # replace it between markers

The markers are the lines `// BEGIN generated: mcp-hub tool reference` and
`// END generated: mcp-hub tool reference`.
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BEGIN = "// BEGIN generated: mcp-hub tool reference"
END = "// END generated: mcp-hub tool reference"


def build(tmp):
    env = dict(os.environ, GOEXPERIMENT="jsonv2")
    # A tagged HEAD is built as that release, so the reference names the
    # version it describes rather than a development build of it.
    tag = subprocess.run(["git", "describe", "--exact-match", "--tags", "HEAD"], cwd=REPO,
                         capture_output=True, text=True).stdout.strip()
    flags = ["-ldflags", "-X github.com/secforge/mcp-hub/internal/version.Release=" + tag] if tag else []
    out = {}
    for name in ("mcp-hub-client", "mcp-hub-server"):
        path = os.path.join(tmp, name)
        subprocess.run(["go", "build"] + flags + ["-o", path, "./cmd/" + name], cwd=REPO, env=env,
                       check=True)
        out[name] = path
    return out


def probe(client, tmp, *, push, project=None, calls=(), wait=3.0):
    """Run the client over stdio and return its JSON-RPC answers by id."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("CLAUDE_CODE_MESSAGING_", "MCP_HUB_", "CODEX_"))}
    # Push mode is chosen by the variable being set. A path nothing listens
    # on keeps the probe from delivering into whatever session runs this.
    # The name is "<parent pid>.sock", as a real harness names it, so the
    # client offers the reply path it would in a real session; nothing
    # listens there. XDG_RUNTIME_DIR keeps the reply inbox it binds out of
    # the directory real sessions discover sockets in.
    env["XDG_RUNTIME_DIR"] = tmp
    if push:
        env["CLAUDE_CODE_MESSAGING_SOCKET"] = os.path.join(tmp, "%d.sock" % os.getpid())
    if project:
        env["MCP_HUB_PROJECT_DIR"] = project
    env["MCP_HUB_CONNSTORE_DIR"] = tempfile.mkdtemp(dir=tmp)
    msgs = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": {"name": "claude-code", "version": "0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    ]
    for i, (tool, args) in enumerate(calls, start=3):
        msgs.append({"jsonrpc": "2.0", "id": i, "method": "tools/call",
                     "params": {"name": tool, "arguments": args}})
    proc = subprocess.Popen([client], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, env=env, cwd=tmp, text=True)
    # Read concurrently: the tool list alone can exceed a pipe buffer, and
    # a client blocked writing never reaches the end of its input.
    lines = []
    reader = threading.Thread(target=lambda: lines.extend(proc.stdout), daemon=True)
    reader.start()
    for m in msgs:
        proc.stdin.write(json.dumps(m) + "\n")
        proc.stdin.flush()
    time.sleep(wait)
    proc.stdin.close()  # EOF is what shuts a stdio MCP server down
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    reader.join(timeout=5)
    answers = {}
    for line in lines:
        m = json.loads(line)
        if "id" in m:
            answers[m["id"]] = m.get("result", m.get("error"))
    return answers


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def literal(text):
    # A literal block shows the text exactly as the model receives it. A
    # line of four dots inside it would end the block, so one is escaped.
    body = "\n".join(("​" + l) if l.strip() == "...." else l for l in text.split("\n"))
    return "....\n" + body + "\n....\n"


def param_type(schema):
    t = schema.get("type", "")
    if t == "array":
        return "array of " + schema.get("items", {}).get("type", "object")
    return t


def render_tool(push_tool, pull_tool):
    tool = push_tool or pull_tool
    out = ["[[tool-%s]]" % tool["name"], "=== `%s`" % tool["name"], ""]
    if push_tool and pull_tool:
        modes = "both delivery modes"
    elif push_tool:
        modes = "push mode only"
    else:
        modes = "pull mode only"
    out.append("_Registered in %s._" % modes)
    out.append("")
    out.append(literal(tool["description"]))
    if push_tool and pull_tool and push_tool["description"] != pull_tool["description"]:
        out.append("In pull mode the description reads:")
        out.append("")
        out.append(literal(pull_tool["description"]))
    ann = tool.get("annotations")
    if ann:
        out.append("Annotations: `%s`" % json.dumps(ann, ensure_ascii=False))
        out.append("")
    schema = tool.get("inputSchema", {})
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    if not props:
        out.append("No parameters.")
        out.append("")
    for name, p in props.items():
        flag = "required" if name in required else "optional"
        out.append("`%s` (%s, %s)::" % (name, param_type(p), flag))
        out.append("+")
        out.append(literal(p.get("description", "")).rstrip("\n"))
        other = pull_tool if tool is push_tool else push_tool
        if other:
            od = other.get("inputSchema", {}).get("properties", {}).get(name, {}).get("description")
            if od is not None and od != p.get("description", ""):
                out.append("+")
                out.append("In pull mode:")
                out.append("+")
                out.append(literal(od).rstrip("\n"))
        out.append("")
    return "\n".join(out)


def startup_notices():
    src = open(os.path.join(REPO, "internal/mcptools/startupnotice.go")).read()
    texts = []
    for m in re.finditer(r'fmt\.Sprintf\(\s*((?:"(?:[^"\\]|\\.)*"\s*\+?\s*)+)', src):
        parts = re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1))
        texts.append(json.loads('"' + "".join(parts) + '"'))
    return texts


def generate():
    tmp = tempfile.mkdtemp(prefix="mcp-hub-toolref-")
    bins = build(tmp)
    push = probe(bins["mcp-hub-client"], tmp, push=True)
    pull = probe(bins["mcp-hub-client"], tmp, push=False)
    scoped = probe(bins["mcp-hub-client"], tmp, push=True, project="/source/example")

    port = free_port()
    logs = tempfile.mkdtemp(dir=tmp)
    server = subprocess.Popen([bins["mcp-hub-server"], "-addr", "127.0.0.1:%d" % port],
                              env=dict(os.environ, MCP_HUB_LOG_DIR=logs),
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(1)
        sid = str(uuid.uuid4())
        link = "ws://127.0.0.1:%d/%s#%s" % (port, sid, sid)
        # A fixed scope, because without one the client asks the MCP
        # client for its roots and this probe does not answer that request.
        connected = probe(bins["mcp-hub-client"], tmp, push=True, project="/source/example",
                          calls=[("hub_connect", {"link": link, "as": "example",
                                                  "name": "Claude Code (example)"})])
    finally:
        server.terminate()
        server.wait()

    version = push[1]["serverInfo"]["version"]
    push_tools = {t["name"]: t for t in push[2]["tools"]}
    pull_tools = {t["name"]: t for t in pull[2]["tools"]}
    names = sorted(set(push_tools) | set(pull_tools))

    out = [BEGIN, "",
           "[[model-reference]]",
           "== Reference: what the model is shown",
           "",
           "Generated by `scripts/tool-reference.py` in the mcp-hub repository from a running "
           "client (`%s`), so this is the exact text a model receives. Delivery framing and "
           "error messages are not included." % version,
           "",
           "*Push mode* is Claude Code or Codex with a messaging channel; *pull mode* is "
           "any other harness. The client sends no server-level instructions at initialize: "
           "what a model learns comes from the tool descriptions below, the connect result, and "
           "the notices it is sent.",
           "",
           "=== Tools", ""]
    out.append(", ".join("<<tool-%s,`%s`>>" % (n, n) for n in names) + ".")
    out.append("")
    for n in names:
        out.append(render_tool(push_tools.get(n), pull_tools.get(n)))

    base = push_tools["hub_connect"]["description"]
    scoped_desc = {t["name"]: t for t in scoped[2]["tools"]}["hub_connect"]["description"]
    out += ["=== Added when `MCP_HUB_PROJECT_DIR` is set", "",
            "`hub_connect`'s description gains this paragraph, here with "
            "`MCP_HUB_PROJECT_DIR=/source/example`:", "",
            literal(scoped_desc[len(base):].strip("\n"))]

    out += ["=== Notices at startup", "",
            "Sent at most once per start, pushed where the harness takes pushes. `%d`/`%s`/`%v` "
            "are filled in at runtime.", ""]
    for text in startup_notices():
        out.append(literal(text))

    result = connected.get(3, {})
    text = "\n".join(c.get("text", "") for c in result.get("content", []))
    out += ["=== Example: a `hub_connect` result", "",
            "Push mode, a first connect to a local `mcp-hub-server` with "
            "`MCP_HUB_PROJECT_DIR=/source/example`. Much of this depends on the "
            "server, the harness and the stored identity — against chat-relay, for instance, it "
            "states the features the server declares — so read it as the shape of a result rather "
            "than a fixed text. The peer id is random.", "",
            literal(text), END]
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--update", metavar="FILE", help="replace the fragment between the markers in FILE")
    args = ap.parse_args()
    frag = generate()
    if not args.update:
        sys.stdout.write(frag)
        return
    doc = open(args.update).read()
    if BEGIN in doc:
        start, end = doc.index(BEGIN), doc.index(END) + len(END) + 1
        doc = doc[:start] + frag + doc[end:]
    else:
        doc = doc.rstrip("\n") + "\n\n" + frag
    open(args.update, "w").write(doc)
    print("updated", args.update)


if __name__ == "__main__":
    main()
