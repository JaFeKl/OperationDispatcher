import json
import shutil
import subprocess

import pytest

from operation_dispatcher.visualization.browser_event_visualizer import _INDEX_HTML


def test_event_renderer_keeps_untrusted_fields_as_text():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to exercise the browser renderer")
    script = _INDEX_HTML.split("<script>", 1)[1].split("</script>", 1)[0]
    # A small DOM harness exercises the shipped renderer without a browser server.
    # An HTML parsing sink is forbidden; data must remain in text nodes.
    harness = r"""
const assert = require('node:assert/strict');
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.text = ''; }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return this.text + this.children.map(c => c.textContent).join(''); }
  set innerHTML(value) { throw new Error('Untrusted HTML reached the DOM parser'); }
  appendChild(child) { this.children.push(child); }
  prepend(child) { this.children.unshift(child); }
}
const timeline = new Element('div');
const document = {
  getElementById: id => id === 'timeline' ? timeline : new Element('div'),
  createElement: tag => new Element(tag),
};
const fetch = () => new Promise(() => {});
"""
    payload = "</pre><img src=x onerror=alert(1)><script>alert(2)</script>"
    event = {
        "timestamp": "2026-01-01T00:00:00Z",
        "source": payload,
        "event_type": payload,
        "resource_id": payload,
        "operation_id": payload,
        "event_details": {"meta_data": {"input": payload}},
        "operation_details": {"payload": {"input": payload}},
    }
    assertions = f"""
const event = {json.dumps(event)};
renderEvent(event);
renderEvent(event);
assert.equal(timeline.children.length, 1);
assert.ok(timeline.textContent.includes(event.resource_id));
const nodes = [];
function visit(node) {{ nodes.push(node); node.children.forEach(visit); }}
visit(timeline);
assert.equal(nodes.filter(n => n.tagName === 'details').length, 2);
assert.equal(nodes.filter(n => n.tagName === 'pre').length, 2);
assert.equal(nodes.find(n => n.className === 'type').textContent, event.event_type);
assert.deepEqual(nodes.filter(n => n.tagName === 'pre').map(n => JSON.parse(n.textContent)),
                 [event.event_details, event.operation_details]);
assert.ok(nodes.every(n => !['img', 'script'].includes(n.tagName)));
"""
    subprocess.run(
        [node, "-e", harness + script + assertions],
        check=True,
        capture_output=True,
        timeout=5,
    )
