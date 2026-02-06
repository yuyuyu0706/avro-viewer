const PAGE_SIZE = 100;

const state = {
  fileName: "",
  schema: null,
  rows: [],
  visibleColumns: [],
  filteredRows: [],
  searchText: "",
  searchField: "__all__",
  filter: null,
  sort: { field: null, dir: "asc" },
  currentPage: 1,
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
  ].forEach((el) => {
    el.disabled = !enabled;
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
  } catch (error) {
    dom.statusText.textContent = "読み込み失敗";
    dom.schemaView.textContent = "(schemaなし)";
    dom.tableHead.innerHTML = "";
    dom.tableBody.innerHTML = "";
    log(`エラー: ${error.message}`, true);
    alert(`Avroの読み込みに失敗しました: ${error.message}`);
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

updateControls(false);
