const PAGE_SIZE = 100;

const state = {
  fileName: "",
  schema: null,
  allRecords: [],
  rows: [],
  visibleColumns: [],
  filteredRows: [],
  searchText: "",
  searchField: "__all__",
  filter: null,
  sort: { field: null, dir: "asc" },
  currentPage: 1,
};

const profileState = {
  worker: null,
  data: null,
  processing: false,
  selectedColumn: null,
  progress: { processed: 0, total: 0 },
};

const dom = {
  fileInput: document.getElementById("fileInput"),
  previewLimit: document.getElementById("previewLimit"),
  statusText: document.getElementById("statusText"),
  schemaView: document.getElementById("schemaView"),
  columnList: document.getElementById("columnList"),
  filterField: document.getElementById("filterField"),
  filterOp: document.getElementById("filterOp"),
  filterValue: document.getElementById("filterValue"),
  applyFilterBtn: document.getElementById("applyFilterBtn"),
  clearFilterBtn: document.getElementById("clearFilterBtn"),
  filterStatus: document.getElementById("filterStatus"),
  dropZone: document.getElementById("dropZone"),
  searchInput: document.getElementById("searchInput"),
  searchField: document.getElementById("searchField"),
  tableHead: document.getElementById("tableHead"),
  tableBody: document.getElementById("tableBody"),
  rowDetail: document.getElementById("rowDetail"),
  prevPageBtn: document.getElementById("prevPageBtn"),
  nextPageBtn: document.getElementById("nextPageBtn"),
  pageInfo: document.getElementById("pageInfo"),
  logArea: document.getElementById("logArea"),
  exportJsonBtn: document.getElementById("exportJsonBtn"),
  exportCsvBtn: document.getElementById("exportCsvBtn"),
  tabBrowse: document.getElementById("tabBrowse"),
  tabProfile: document.getElementById("tabProfile"),
  browsePanel: document.getElementById("browsePanel"),
  profilePanel: document.getElementById("profilePanel"),
  profileStatus: document.getElementById("profileStatus"),
  topKInput: document.getElementById("topKInput"),
  suspiciousList: document.getElementById("suspiciousList"),
  profileDetail: document.getElementById("profileDetail"),
};

function log(message, isError = false) {
  const stamp = new Date().toLocaleTimeString();
  dom.logArea.textContent = `ログ [${stamp}] ${message}`;
  if (isError) console.error(message);
}

function stringifyCell(value) {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function detectColumns(rows) {
  const set = new Set();
  for (const row of rows) {
    Object.keys(row || {}).forEach((k) => set.add(k));
  }
  return [...set];
}

function parseAvroWithAvsc(buffer) {
  const bytes = new Uint8Array(buffer);
  if (!window.avsc?.streams?.BlockDecoder) {
    throw new Error("avsc BlockDecoder が利用できません。CDN読み込みを確認してください。");
  }

  const decoder = new window.avsc.streams.BlockDecoder();
  const chunk = window.avsc.utils?.toBuffer ? window.avsc.utils.toBuffer(bytes) : bytes;

  return new Promise((resolve, reject) => {
    const records = [];
    let schema = null;

    decoder.on("metadata", (type) => {
      try {
        schema = type?.schema ? type.schema({ exportAttrs: true }) : null;
      } catch (error) {
        log(`schema抽出に失敗: ${error.message}`, true);
      }
    });

    decoder.on("data", (record) => records.push(record));
    decoder.on("error", (error) => reject(error));
    decoder.on("end", () => resolve({ records, schema }));

    decoder.end(chunk);
  });
}

function buildColumnSelectors(columns) {
  dom.columnList.innerHTML = "";
  dom.searchField.innerHTML = '<option value="__all__">全フィールド</option>';
  dom.filterField.innerHTML = "";

  for (const col of columns) {
    const wrap = document.createElement("label");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = true;
    checkbox.dataset.field = col;
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) {
        state.visibleColumns.push(col);
      } else {
        state.visibleColumns = state.visibleColumns.filter((c) => c !== col);
      }
      renderTable();
    });
    wrap.append(checkbox, ` ${col}`);
    dom.columnList.appendChild(wrap);

    const searchOpt = document.createElement("option");
    searchOpt.value = col;
    searchOpt.textContent = col;
    dom.searchField.appendChild(searchOpt);

    const filterOpt = document.createElement("option");
    filterOpt.value = col;
    filterOpt.textContent = col;
    dom.filterField.appendChild(filterOpt);
  }
}

function applyAllTransforms() {
  let rows = [...state.rows];

  if (state.filter?.field) {
    rows = rows.filter((row) => {
      const raw = row[state.filter.field];
      const val = stringifyCell(raw).toLowerCase();
      const expected = (state.filter.value || "").toLowerCase();
      if (state.filter.op === "exists") return raw !== undefined && raw !== null;
      if (state.filter.op === "eq") return val === expected;
      return val.includes(expected);
    });
  }

  if (state.searchText) {
    const text = state.searchText.toLowerCase();
    rows = rows.filter((row) => {
      if (state.searchField === "__all__") {
        return state.visibleColumns.some((field) => stringifyCell(row[field]).toLowerCase().includes(text));
      }
      return stringifyCell(row[state.searchField]).toLowerCase().includes(text);
    });
  }

  if (state.sort.field) {
    rows.sort((a, b) => {
      const left = stringifyCell(a[state.sort.field]);
      const right = stringifyCell(b[state.sort.field]);
      const result = left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" });
      return state.sort.dir === "asc" ? result : -result;
    });
  }

  state.filteredRows = rows;
}

function renderTable() {
  applyAllTransforms();

  const totalPages = Math.max(1, Math.ceil(state.filteredRows.length / PAGE_SIZE));
  state.currentPage = Math.min(state.currentPage, totalPages);
  const start = (state.currentPage - 1) * PAGE_SIZE;
  const pageRows = state.filteredRows.slice(start, start + PAGE_SIZE);

  dom.tableHead.innerHTML = "";
  const headRow = document.createElement("tr");
  for (const col of state.visibleColumns) {
    const th = document.createElement("th");
    const icon = state.sort.field === col ? (state.sort.dir === "asc" ? " ▲" : " ▼") : "";
    th.textContent = `${col}${icon}`;
    th.addEventListener("click", () => {
      if (state.sort.field === col) {
        state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
      } else {
        state.sort.field = col;
        state.sort.dir = "asc";
      }
      renderTable();
    });
    headRow.appendChild(th);
  }
  dom.tableHead.appendChild(headRow);

  dom.tableBody.innerHTML = "";
  for (const row of pageRows) {
    const tr = document.createElement("tr");
    tr.addEventListener("click", () => {
      dom.rowDetail.textContent = JSON.stringify(row, null, 2);
    });

    for (const col of state.visibleColumns) {
      const td = document.createElement("td");
      td.textContent = stringifyCell(row[col]);
      tr.appendChild(td);
    }
    dom.tableBody.appendChild(tr);
  }

  dom.pageInfo.textContent = `${state.currentPage} / ${totalPages}（${state.filteredRows.length}件）`;
  dom.prevPageBtn.disabled = state.currentPage <= 1;
  dom.nextPageBtn.disabled = state.currentPage >= totalPages;
}

function updateControls(enabled) {
  [
    dom.searchInput,
    dom.searchField,
    dom.prevPageBtn,
    dom.nextPageBtn,
    dom.exportJsonBtn,
    dom.exportCsvBtn,
    dom.applyFilterBtn,
    dom.clearFilterBtn,
    dom.topKInput,
  ].forEach((el) => {
    el.disabled = !enabled;
  });
}

function setActiveTab(tab) {
  const isBrowse = tab === "browse";
  dom.tabBrowse.classList.toggle("active", isBrowse);
  dom.tabProfile.classList.toggle("active", !isBrowse);
  dom.browsePanel.classList.toggle("active", isBrowse);
  dom.profilePanel.classList.toggle("active", !isBrowse);
}

function formatRate(rate) {
  if (Number.isNaN(rate)) return "-";
  return `${(rate * 100).toFixed(1)}%`;
}

function formatCount(value) {
  return value?.toLocaleString() ?? "-";
}

function renderProfileStatus(message) {
  dom.profileStatus.textContent = message;
}

function clearProfileView(message) {
  dom.suspiciousList.textContent = message;
  dom.profileDetail.textContent = message;
}

function renderProfileRanking() {
  const ranking = profileState.data?.suspiciousRanking || [];
  dom.suspiciousList.innerHTML = "";

  if (!ranking.length) {
    dom.suspiciousList.textContent = "(ランキングなし)";
    return;
  }

  ranking.forEach((item, index) => {
    const wrap = document.createElement("div");
    wrap.className = "profile-item";
    wrap.dataset.column = item.column;
    if (!profileState.selectedColumn && index === 0) {
      profileState.selectedColumn = item.column;
    }
    if (profileState.selectedColumn === item.column) {
      wrap.classList.add("active");
    }

    const title = document.createElement("div");
    title.textContent = `${item.column} (score ${item.score})`;
    title.style.fontWeight = "600";

    const reasonList = document.createElement("ul");
    reasonList.style.margin = "0.35rem 0 0";
    reasonList.style.paddingLeft = "1.2rem";
    item.reasons.forEach((reason) => {
      const li = document.createElement("li");
      li.textContent = reason.message;
      reasonList.appendChild(li);
    });

    wrap.append(title, reasonList);
    wrap.addEventListener("click", () => {
      profileState.selectedColumn = item.column;
      renderProfileRanking();
      renderProfileDetail();
    });
    dom.suspiciousList.appendChild(wrap);
  });
}

function renderProfileDetail() {
  const column = profileState.selectedColumn;
  const data = profileState.data?.columns?.[column];
  dom.profileDetail.innerHTML = "";

  if (!column || !data) {
    dom.profileDetail.textContent = "(列を選択してください)";
    return;
  }

  const header = document.createElement("div");
  header.innerHTML = `<strong>${column}</strong>`;

  const grid = document.createElement("div");
  grid.className = "profile-grid";

  const cards = [
    { title: "推定型", value: data.typeHint },
    { title: "総レコード", value: formatCount(profileState.data.totalRecords) },
    { title: "NULL件数", value: formatCount(data.nullCount) },
    { title: "NULL率", value: formatRate(data.nullRate) },
    { title: "非NULL件数", value: formatCount(data.nonNullCount) },
  ];

  if (data.min !== undefined && data.max !== undefined) {
    const minVal = data.minDisplay ?? data.min;
    const maxVal = data.maxDisplay ?? data.max;
    cards.push({ title: "min", value: minVal });
    cards.push({ title: "max", value: maxVal });
  } else if (data.minMaxReason) {
    cards.push({ title: "min/max", value: data.minMaxReason });
  }

  if (data.topKLimited) {
    cards.push({ title: "TopK注意", value: "ユニーク値が多く精度低下" });
  }

  cards.forEach((card) => {
    const box = document.createElement("div");
    box.className = "profile-card";
    box.innerHTML = `<h3>${card.title}</h3><div>${card.value}</div>`;
    grid.appendChild(box);
  });

  const table = document.createElement("table");
  table.className = "profile-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>TopK</th>
        <th>件数</th>
        <th>割合</th>
      </tr>
    </thead>
  `;
  const tbody = document.createElement("tbody");
  if (data.topK?.length) {
    data.topK.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.value}</td>
        <td>${formatCount(item.count)}</td>
        <td>${formatRate(item.rate)}</td>
      `;
      tbody.appendChild(row);
    });
  } else {
    const row = document.createElement("tr");
    row.innerHTML = `<td colspan="3">TopKなし</td>`;
    tbody.appendChild(row);
  }
  table.appendChild(tbody);

  dom.profileDetail.append(header, grid, table);
}

function renderProfile() {
  if (!profileState.data) {
    clearProfileView("(未解析)");
    return;
  }
  renderProfileRanking();
  renderProfileDetail();
}

function stopProfileWorker() {
  if (profileState.worker) {
    profileState.worker.terminate();
    profileState.worker = null;
  }
}

function startProfileWorker(records, schema) {
  stopProfileWorker();
  profileState.processing = true;
  profileState.data = null;
  profileState.selectedColumn = null;
  profileState.progress = { processed: 0, total: records.length };
  renderProfileStatus("Profile解析中…");
  clearProfileView("(解析中)");

  const worker = new Worker(new URL("./workers/profileWorker.js", import.meta.url), { type: "module" });
  profileState.worker = worker;

  worker.addEventListener("message", (event) => {
    const { type, payload } = event.data || {};
    if (type === "PROGRESS") {
      profileState.progress = payload;
      renderProfileStatus(`Profile解析中… ${payload.processedRecords.toLocaleString()} / ${payload.totalRecords.toLocaleString()}`);
      return;
    }
    if (type === "RESULT") {
      profileState.processing = false;
      profileState.data = payload;
      renderProfileStatus("Profile解析完了");
      renderProfile();
      return;
    }
    if (type === "ERROR") {
      profileState.processing = false;
      renderProfileStatus("Profile解析失敗");
      clearProfileView(payload?.message || "Profile解析に失敗しました");
    }
  });

  worker.postMessage({
    type: "START",
    payload: {
      records,
      schema,
      topK: Number(dom.topKInput.value) || 10,
    },
  });
}

function download(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function exportJson() {
  const payload = JSON.stringify(state.filteredRows, null, 2);
  const base = state.fileName.replace(/\.avro$/i, "") || "records";
  download(`${base}.json`, payload, "application/json");
}

function exportCsv() {
  const header = state.visibleColumns.join(",");
  const body = state.filteredRows
    .map((row) =>
      state.visibleColumns
        .map((col) => {
          const value = stringifyCell(row[col]).replaceAll('"', '""');
          return `"${value}"`;
        })
        .join(","),
    )
    .join("\n");
  const base = state.fileName.replace(/\.avro$/i, "") || "records";
  download(`${base}.csv`, `${header}\n${body}`, "text/csv;charset=utf-8");
}

function resetForNewFile(fileName) {
  state.fileName = fileName;
  state.schema = null;
  state.allRecords = [];
  state.rows = [];
  state.visibleColumns = [];
  state.filteredRows = [];
  state.searchText = "";
  state.filter = null;
  state.sort = { field: null, dir: "asc" };
  state.currentPage = 1;
  dom.rowDetail.textContent = "行をクリックするとJSONが表示されます";
  dom.searchInput.value = "";
  dom.filterValue.value = "";
  dom.filterStatus.textContent = "フィルタ未適用";
  updateControls(false);
  stopProfileWorker();
  profileState.data = null;
  profileState.processing = false;
  profileState.selectedColumn = null;
  renderProfileStatus("Profile未実行");
  clearProfileView("(未解析)");
}

async function handleFile(file) {
  if (!file) return;
  resetForNewFile(file.name);
  dom.statusText.textContent = `${file.name} を読み込み中...`;
  log(`読み込み開始: ${file.name}`);

  try {
    const buffer = await file.arrayBuffer();
    const { records, schema } = await parseAvroWithAvsc(buffer);

    const previewLimit = Number(dom.previewLimit.value) || 200;
    state.allRecords = records;
    state.rows = records.slice(0, previewLimit);
    state.schema = schema || { note: "schema を取得できませんでした" };
    state.visibleColumns = detectColumns(state.rows);

    if (!state.rows.length) {
      throw new Error("レコードが0件でした。");
    }

    dom.schemaView.textContent = JSON.stringify(state.schema, null, 2);
    buildColumnSelectors(state.visibleColumns);
    state.searchField = "__all__";
    renderTable();
    updateControls(true);
    dom.statusText.textContent = `${file.name}: ${records.length}件読み込み（表示: ${state.rows.length}件）`;
    log(`読み込み完了: ${records.length}件`);
    startProfileWorker(state.allRecords, state.schema);
  } catch (error) {
    dom.statusText.textContent = "読み込み失敗";
    dom.schemaView.textContent = "(schemaなし)";
    dom.tableHead.innerHTML = "";
    dom.tableBody.innerHTML = "";
    log(`エラー: ${error.message}`, true);
    alert(`Avroの読み込みに失敗しました: ${error.message}`);
    renderProfileStatus("Profile解析失敗");
    clearProfileView(error.message);
  }
}

dom.fileInput.addEventListener("change", (event) => {
  handleFile(event.target.files?.[0]);
});

["dragenter", "dragover"].forEach((eventName) => {
  dom.dropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    dom.dropZone.classList.add("drag-over");
  });
});

["dragleave", "drop"].forEach((eventName) => {
  dom.dropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    dom.dropZone.classList.remove("drag-over");
  });
});

dom.dropZone.addEventListener("drop", (event) => {
  const [file] = event.dataTransfer?.files || [];
  handleFile(file);
});

dom.searchInput.addEventListener("input", () => {
  state.searchText = dom.searchInput.value.trim();
  state.currentPage = 1;
  renderTable();
});

dom.searchField.addEventListener("change", () => {
  state.searchField = dom.searchField.value;
  state.currentPage = 1;
  renderTable();
});

dom.applyFilterBtn.addEventListener("click", () => {
  state.filter = {
    field: dom.filterField.value,
    op: dom.filterOp.value,
    value: dom.filterValue.value,
  };
  state.currentPage = 1;
  dom.filterStatus.textContent = `適用中: ${state.filter.field} ${state.filter.op} ${state.filter.value}`;
  renderTable();
});

dom.clearFilterBtn.addEventListener("click", () => {
  state.filter = null;
  state.currentPage = 1;
  dom.filterStatus.textContent = "フィルタ未適用";
  renderTable();
});

dom.prevPageBtn.addEventListener("click", () => {
  state.currentPage -= 1;
  renderTable();
});

dom.nextPageBtn.addEventListener("click", () => {
  state.currentPage += 1;
  renderTable();
});

dom.exportJsonBtn.addEventListener("click", exportJson);
dom.exportCsvBtn.addEventListener("click", exportCsv);

dom.tabBrowse.addEventListener("click", () => setActiveTab("browse"));
dom.tabProfile.addEventListener("click", () => setActiveTab("profile"));

dom.topKInput.addEventListener("change", () => {
  if (!state.allRecords.length || profileState.processing) return;
  startProfileWorker(state.allRecords, state.schema);
});

updateControls(false);
setActiveTab("browse");
clearProfileView("(未解析)");
