<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import { loadDeptList, type DeptItem } from '$lib/utils/deptSummaries';

	interface Props {
		analysisId: string;
		territoryPrefix: string;
		onSelect: (name: string, parquetKey: string) => void;
		onClose?: () => void;
	}

	let { analysisId, territoryPrefix, onSelect, onClose }: Props = $props();

	let depts = $state<DeptItem[]>([]);
	let loading = $state(true);
	let search = $state('');

	$effect(() => {
		loading = true;
		depts = [];
		loadDeptList(analysisId, territoryPrefix).then(list => {
			depts = list;
			loading = false;
		});
	});

	const filtered = $derived(
		search.trim()
			? depts.filter(d => d.name.toLowerCase().includes(search.toLowerCase()))
			: depts
	);

	function handleSelect(d: DeptItem) {
		onSelect(d.name, d.parquetKey);
	}
</script>

<div class="dept-panel">
	<div class="panel-header">
		<span class="panel-title">{i18n.t('panel.selectDistrict')}</span>
		{#if onClose}
			<button class="close-btn" onclick={onClose}>✕</button>
		{/if}
	</div>

	{#if depts.length > 6}
		<input
			class="search-input"
			type="text"
			placeholder="Buscar..."
			bind:value={search}
		/>
	{/if}

	<div class="dept-list">
		{#if loading}
			<div class="hint">{i18n.t('common.loading')}</div>
		{:else if filtered.length === 0}
			<div class="hint">{i18n.t('common.noResults')}</div>
		{:else}
			{#each filtered as d (d.parquetKey)}
				<button class="dept-item" onclick={() => handleSelect(d)}>
					{d.name}
				</button>
			{/each}
		{/if}
	</div>
</div>

<style>
	.dept-panel {
		position: absolute;
		bottom: 32px;
		right: 8px;
		width: 190px;
		max-height: 340px;
		background: rgba(15, 23, 42, 0.92);
		border: 1px solid rgba(148, 163, 184, 0.15);
		border-radius: 6px;
		display: flex;
		flex-direction: column;
		z-index: 10;
		backdrop-filter: blur(4px);
		font-size: 11px;
		color: #e2e8f0;
	}

	.panel-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 7px 9px 5px;
		border-bottom: 1px solid rgba(148, 163, 184, 0.12);
		flex-shrink: 0;
	}

	.panel-title {
		font-size: 10px;
		color: #94a3b8;
		text-transform: uppercase;
		letter-spacing: 0.04em;
	}

	.close-btn {
		background: none;
		border: none;
		color: #64748b;
		cursor: pointer;
		padding: 0;
		font-size: 10px;
		line-height: 1;
	}
	.close-btn:hover { color: #94a3b8; }

	.search-input {
		margin: 5px 8px 3px;
		padding: 4px 7px;
		background: rgba(30, 41, 59, 0.8);
		border: 1px solid rgba(148, 163, 184, 0.2);
		border-radius: 4px;
		color: #e2e8f0;
		font-size: 10px;
		outline: none;
		flex-shrink: 0;
	}
	.search-input::placeholder { color: #475569; }
	.search-input:focus { border-color: rgba(244, 114, 182, 0.4); }

	.dept-list {
		overflow-y: auto;
		padding: 3px 4px 4px;
		display: flex;
		flex-direction: column;
		gap: 1px;
	}

	.dept-item {
		background: none;
		border: none;
		text-align: left;
		padding: 5px 8px;
		color: #cbd5e1;
		cursor: pointer;
		border-radius: 4px;
		font-size: 11px;
		transition: background 0.1s, color 0.1s;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	.dept-item:hover {
		background: rgba(244, 114, 182, 0.12);
		color: #f9a8d4;
	}

	.hint {
		padding: 8px;
		color: #64748b;
		font-size: 10px;
	}
</style>
