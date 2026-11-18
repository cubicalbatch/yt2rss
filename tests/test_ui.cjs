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

function stubRedownload(context, jobStates, redownloadResponse) {
    const harness = { toasts: [], timers: [], requests: [] };
    context.confirm = () => true;
    context.showToast = message => harness.toasts.push(message);
    context.setTimeout = (callback, delay) => harness.timers.push({ callback, delay });
    context.fetch = async (url, options = {}) => {
        harness.requests.push({ url, method: options.method || 'GET' });
        if (url.endsWith('/redownload')) return redownloadResponse;
        if (url === '/api/queue/job1') {
            const job = jobStates.shift();
            return { ok: true, status: 200, json: async () => job };
        }
        return { ok: true, status: 200, json: async () => ({ jobs: [], summary: {} }) };
    };
    harness.runTimer = async expectedDelay => {
        const timer = harness.timers.shift();
        assert.equal(timer.delay, expectedDelay);
        await timer.callback();
    };
    harness.pollCount = () => harness.requests.filter(r => r.url === '/api/queue/job1').length;
    return harness;
}

const QUEUED = {
    ok: true,
    status: 202,
    json: async () => ({ message: 'queued', job: { id: 'job1', kind: 'redownload', state: 'pending' } }),
};

function redownloadButton() {
    const button = new Element('button');
    button.textContent = '🔄 Redownload';
    return button;
}

test('redownload queues a job and follows it to completion', async () => {
    const { context } = setup();
    const harness = stubRedownload(context, [
        { id: 'job1', state: 'pending', attempts: 0 },
        { id: 'job1', state: 'running', progress: { percent: 42.4 } },
        { id: 'job1', state: 'completed', title: 'Episode' },
    ], QUEUED);
    const button = redownloadButton();

    await context.redownloadEpisode('channel', 'vid 1', 'Episode', button);
    assert.equal(button.textContent, '⏳ Queued');
    assert.equal(button.disabled, true);
    assert.deepEqual(harness.requests.slice(0, 2), [
        { url: '/api/channels/channel/episodes/vid%201/redownload', method: 'POST' },
        { url: '/api/queue', method: 'GET' },
    ]);

    await harness.runTimer(2000);
    assert.equal(button.textContent, '⏳ Queued');
    await harness.runTimer(2000);
    assert.equal(button.textContent, '⏳ 42%');
    await harness.runTimer(2000);
    assert.equal(button.textContent, '✅ Redownloaded!');
    assert.equal(harness.pollCount(), 3);
    assert.equal(harness.requests.at(-1).url, '/api/queue');

    await harness.runTimer(2000);
    assert.equal(button.textContent, '🔄 Redownload');
    assert.equal(button.disabled, false);
    assert.equal(harness.timers.length, 0, 'polling stops on a terminal state');
});

test('failed redownload job shows its error and re-enables the button', async () => {
    const { context } = setup();
    const harness = stubRedownload(context, [
        { id: 'job1', state: 'failed', error: 'Redownload failed; original episode preserved' },
    ], QUEUED);
    const button = redownloadButton();

    await context.redownloadEpisode('channel', 'vid1', 'Episode', button);
    await harness.runTimer(2000);
    assert.equal(button.textContent, '❌ Failed');
    assert.equal(button.title, 'Redownload failed; original episode preserved');
    assert.match(harness.toasts.at(-1), /original episode preserved/);

    await harness.runTimer(5000);
    assert.equal(button.textContent, '🔄 Redownload');
    assert.equal(button.disabled, false);
    assert.equal(harness.timers.length, 0);
});

test('rejected redownload request reports the server error', async () => {
    const { context } = setup();
    const harness = stubRedownload(context, [], {
        ok: false,
        status: 409,
        json: async () => ({ error: 'This episode is already queued for download' }),
    });
    const button = redownloadButton();

    await context.redownloadEpisode('channel', 'vid1', 'Episode', button);
    assert.equal(button.textContent, '❌ Failed');
    assert.match(harness.toasts.at(-1), /already queued/);
    await harness.runTimer(3000);
    assert.equal(button.textContent, '🔄 Redownload');
    assert.equal(harness.pollCount(), 0);
});

test('redownload polling stops once the modal or button goes away', async () => {
    for (const hide of [
        context => context.closeEpisodesModal(),
        (context, button) => { button.isConnected = false; },
    ]) {
        const { context } = setup();
        const harness = stubRedownload(context, [{ id: 'job1', state: 'running' }], QUEUED);
        const button = redownloadButton();
        await context.redownloadEpisode('channel', 'vid1', 'Episode', button);
        hide(context, button);
        await harness.runTimer(2000);
        assert.equal(harness.pollCount(), 0);
        assert.equal(harness.timers.length, 0);
    }
});

test('queue panel labels redownload jobs', () => {
    const { context } = setup();
    const header = job => context.buildQueueItem(job).children[0].children.map(child => child.textContent);
    const base = { id: 'job1', channel: 'channel', title: 'Episode', state: 'pending', attempts: 0 };
    assert.deepEqual(header({ ...base, kind: 'redownload' }), ['Episode', '🔄 Redownload', 'pending']);
    assert.deepEqual(header({ ...base, kind: 'download' }), ['Episode', 'pending']);
});
