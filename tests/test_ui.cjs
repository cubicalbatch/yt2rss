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
        this._textContent = '';
    }
    set textContent(value) {
        this._textContent = value;
        if (value === '') this.children = [];
    }
    get textContent() { return this._textContent; }
    append(...children) { this.children.push(...children); }
    appendChild(child) { this.children.push(child); }
    replaceChildren(...children) { this.children = children; }
    addEventListener(event, listener) { this.listeners[event] = listener; }
    set innerHTML(html) {
        // Only fixed loading/error markup should use this sink in these cases.
        assert.ok(!html.includes('pwned'), 'Untrusted metadata reached innerHTML');
        this.children = [];
    }
}

function setup(overrides = {}) {
    const elements = new Map();
    const episode = {
        id: '_video123',
        title: "');globalThis.pwned=true;//",
        date: '<img src=x onerror=pwned()>',
        duration: '<svg onload=pwned()>',
        description: '<script>pwned()</script>',
        file_extension: '.webm',
        ...overrides,
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

function renderButtons(context, elements) {
    const item = elements.get('episodesContent').children[0].children[0];
    return item.children[1].children;
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

test('all six episode actions receive exact values and their button', async () => {
    const { context, elements, episode } = setup();
    const calls = [];
    for (const name of ['openEpisodeMedia', 'downloadAsMP3', 'downloadOriginalFile', 'redownloadEpisode', 'deleteEpisode', 'togglePin']) {
        context[name] = (...args) => calls.push({ name, args });
    }
    await context.openEpisodesModal('channel', 'Display Name');
    const buttons = renderButtons(context, elements);
    assert.equal(buttons[5].textContent, '📌 Pin');
    buttons.forEach(button => button.listeners.click());
    assert.equal(calls.length, 6);
    calls.forEach((call, index) => {
        assert.equal(call.args[0], 'channel');
        assert.equal(call.args[1], episode.id);
        if (index < 3) assert.equal(call.args[2], '.webm');
        else if (index < 5) assert.equal(call.args[2], episode.title);
        else assert.equal(call.args[2], false);
        if (index > 0) assert.equal(call.args[3], buttons[index]);
    });
    assert.equal(context.pwned, undefined);
});

test('pinned episodes show a badge and offer unpinning', async () => {
    const { context, elements } = setup({ pinned: true });
    const toggles = [];
    context.togglePin = (channel, id, pinned) => toggles.push(pinned);
    await context.openEpisodesModal('channel', 'Display Name');
    const item = elements.get('episodesContent').children[0].children[0];
    const badge = item.children[0].children[0].children[2];
    assert.equal(badge.textContent, '📌');
    const buttons = renderButtons(context, elements);
    assert.equal(buttons[5].textContent, '📍 Unpin');
    buttons[5].listeners.click();
    assert.deepEqual(toggles, [true]);
});

test('health panel renders channel state, disk and pending alerts', async () => {
    const { context, elements } = setup();
    context.fetch = async () => ({
        ok: true,
        json: async () => ({
            channels: {
                example: {
                    consecutive_failures: 3,
                    unavailable: false,
                    last_success_at: null,
                    last_failure_at: '2026-01-01T00:00:00Z',
                    last_error: 'boom',
                    history: [],
                },
            },
            notifications: { settings: { webhook_url: '' }, last_error: null },
            disk: { free_bytes: 1024 ** 3, min_free_space_gb: 2 },
            pending_events: [
                { event: 'channel_failure', message: 'Channel failed three times' },
            ],
        }),
    });
    await context.refreshHealthPanel();
    const row = elements.get('healthChannels').children[0];
    assert.equal(row.children[0].textContent, 'example');
    assert.match(row.children[1].textContent, /3 consecutive failure/);
    assert.equal(row.className, 'health-row degraded');
    assert.match(elements.get('healthDisk').textContent, /1\.00 GB/);
    assert.equal(elements.get('healthDisk').className, 'health-disk low');
    assert.match(elements.get('healthSummary').textContent, /1 issue/);
    assert.ok(elements.get('healthNotifications').children.some(
        child => (child.textContent || '').includes('Channel failed three times')
    ));
});
