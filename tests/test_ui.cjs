// Exercise metadata rendering and button bindings without external DOM packages.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { test } = require('node:test');

class Element {
    constructor(tag) {
        this.tag = tag;
        this.children = [];
        this.style = {};
        this.listeners = {};
        this.textContent = '';
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = children; }
    addEventListener(event, listener) { this.listeners[event] = listener; }
    set innerHTML(html) {
        // Only fixed loading/error markup should use this sink in these cases.
        assert.ok(!html.includes('pwned'), 'Untrusted metadata reached innerHTML');
        this.children = [];
    }
}

function setup() {
    const elements = new Map();
    const episode = {
        id: '_video123',
        title: "');globalThis.pwned=true;//",
        date: '<img src=x onerror=pwned()>',
        duration: '<svg onload=pwned()>',
        description: '<script>pwned()</script>',
        file_extension: '.webm',
    };
    const context = vm.createContext({
        console,
        document: {
            getElementById(id) {
                if (!elements.has(id)) elements.set(id, new Element('div'));
                return elements.get(id);
            },
            createElement: tag => new Element(tag),
            addEventListener() {},
        },
        window: {},
        fetch: async () => ({ ok: true, json: async () => ({ episodes: [episode] }) }),
    });
    vm.runInContext(fs.readFileSync('src/static/js/script.js', 'utf8'), context);
    return { context, elements, episode };
}

test('episode metadata is rendered as text', async () => {
    const { context, elements, episode } = setup();
    await context.openEpisodesModal('channel', 'Display Name');
    const item = elements.get('episodesContent').children[0].children[0];
    const [content] = item.children;
    assert.equal(content.children[0].children[0].textContent, episode.title);
    assert.equal(content.children[0].children[1].children[0].textContent, episode.date);
    assert.equal(content.children[0].children[1].children[1].textContent, episode.duration);
    assert.equal(content.children[1].textContent, episode.description);
    assert.equal(context.pwned, undefined);
});

test('all five episode actions receive exact values and their button', async () => {
    const { context, elements, episode } = setup();
    const calls = [];
    for (const name of ['openEpisodeMedia', 'downloadAsMP3', 'downloadOriginalFile', 'redownloadEpisode', 'deleteEpisode']) {
        context[name] = (...args) => calls.push({ name, args });
    }
    await context.openEpisodesModal('channel', 'Display Name');
    const buttons = elements.get('episodesContent').children[0].children[0].children[1].children;
    buttons.forEach(button => button.listeners.click());
    assert.equal(calls.length, 5);
    calls.forEach((call, index) => {
        assert.equal(call.args[0], 'channel');
        assert.equal(call.args[1], episode.id);
        assert.equal(call.args[2], index < 3 ? '.webm' : episode.title);
        if (index > 0) assert.equal(call.args[3], buttons[index]);
    });
    assert.equal(context.pwned, undefined);
});
