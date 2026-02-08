const TYPE_SAMPLE_LIMIT = 2000;
const MAX_FREQ_MAP_SIZE = 50000;
const NUMERIC_THRESHOLD = 0.8;
const SCORE_CONFIG = {
  nullRateThreshold: 0.3,
  nullRateMax: 40,
  top1RateThreshold: 0.95,
  top1RateMax: 30,
  flatValueScore: 10,
  freqOverflowScore: 8,
};

function isPlainObject(value) {
  return Object.prototype.toString.call(value) === "[object Object]";
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (error) {
    return "[Unstringifiable]";
  }
}

function normalizeValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value) || isPlainObject(value)) {
    return safeStringify(value);
  }
  return String(value);
}

function truncateValue(value, maxLength = 200) {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}…`;
}

function detectLogicalTypes(schema) {
  const map = {};
  if (!schema || !Array.isArray(schema.fields)) return map;

  for (const field of schema.fields) {
    const typeDef = field.type;
    const logicalType = extractLogicalType(typeDef);
    if (logicalType) {
      map[field.name] = logicalType;
    }
  }
  return map;
}

function extractLogicalType(typeDef) {
  if (!typeDef) return null;
  if (Array.isArray(typeDef)) {
    for (const item of typeDef) {
      const logical = extractLogicalType(item);
      if (logical) return logical;
    }
    return null;
  }
  if (typeof typeDef === "object") {
    if (typeDef.logicalType) return typeDef.logicalType;
    return extractLogicalType(typeDef.type);
  }
  return null;
}

function initColumnStats(columns) {
  const stats = {};
  for (const col of columns) {
    stats[col] = {
      nullCount: 0,
      nonNullCount: 0,
      typeCounts: {
        string: 0,
        number: 0,
        boolean: 0,
        object: 0,
        array: 0,
        other: 0,
      },
      numericCount: 0,
      numericMin: null,
      numericMax: null,
      dateMin: null,
      dateMax: null,
      freqMap: new Map(),
      freqOverflow: false,
    };
  }
  return stats;
}

function updateMinMax(stats, value) {
  if (stats.numericMin === null || value < stats.numericMin) {
    stats.numericMin = value;
  }
  if (stats.numericMax === null || value > stats.numericMax) {
    stats.numericMax = value;
  }
}

function updateDateMinMax(stats, value) {
  if (stats.dateMin === null || value < stats.dateMin) {
    stats.dateMin = value;
  }
  if (stats.dateMax === null || value > stats.dateMax) {
    stats.dateMax = value;
  }
}

function updateTypeCounts(stats, value) {
  if (value === null || value === undefined) return;
  if (Array.isArray(value)) {
    stats.typeCounts.array += 1;
    return;
  }
  switch (typeof value) {
    case "string":
      stats.typeCounts.string += 1;
      break;
    case "number":
      stats.typeCounts.number += 1;
      break;
    case "boolean":
      stats.typeCounts.boolean += 1;
      break;
    case "object":
      stats.typeCounts.object += 1;
      break;
    default:
      stats.typeCounts.other += 1;
  }
}

function updateFrequency(stats, value) {
  if (value === null || value === undefined) return;
  const normalized = normalizeValue(value);
  if (normalized === null) return;
  const key = truncateValue(normalized);

  if (stats.freqMap.has(key)) {
    stats.freqMap.set(key, stats.freqMap.get(key) + 1);
    return;
  }

  if (stats.freqOverflow) return;
  if (stats.freqMap.size >= MAX_FREQ_MAP_SIZE) {
    stats.freqOverflow = true;
    return;
  }

  stats.freqMap.set(key, 1);
}

function computeTypeHint(stats, logicalType) {
  const total = stats.nonNullCount;
  if (!total) return "unknown";
  if (logicalType === "timestamp-millis" || logicalType === "timestamp-micros") {
    return "datetime";
  }

  const entries = Object.entries(stats.typeCounts);
  entries.sort((a, b) => b[1] - a[1]);
  const [topType, topCount] = entries[0];
  const ratio = topCount / total;

  if (ratio >= 0.8) return topType;
  return "mixed";
}

function computeTopK(stats, nonNullCount, topK) {
  const entries = [...stats.freqMap.entries()];
  entries.sort((a, b) => b[1] - a[1]);
  return entries.slice(0, topK).map(([value, count]) => ({
    value,
    count,
    rate: nonNullCount ? count / nonNullCount : 0,
  }));
}

function buildSuspiciousRanking(columns, config) {
  const ranking = [];

  for (const [column, stats] of Object.entries(columns)) {
    let score = 0;
    const reasons = [];

    if (stats.nullRate >= config.nullRateThreshold) {
      const ratio = (stats.nullRate - config.nullRateThreshold) / (1 - config.nullRateThreshold);
      const add = Math.round(Math.min(config.nullRateMax, config.nullRateMax * ratio));
      score += add;
      reasons.push({
        code: "HIGH_NULL_RATE",
        weight: add,
        message: `NULL率が${(stats.nullRate * 100).toFixed(1)}%と高い`,
      });
    }

    if (stats.top1Rate >= config.top1RateThreshold) {
      const ratio = (stats.top1Rate - config.top1RateThreshold) / (1 - config.top1RateThreshold);
      const add = Math.round(Math.min(config.top1RateMax, config.top1RateMax * ratio));
      score += add;
      reasons.push({
        code: "TOP1_DOMINANT",
        weight: add,
        message: `Top1が${(stats.top1Rate * 100).toFixed(1)}%を占める`,
      });
    }

    if (stats.minMaxFlat) {
      score += config.flatValueScore;
      reasons.push({
        code: "MIN_EQ_MAX",
        weight: config.flatValueScore,
        message: "min/maxが同一（値が固定）",
      });
    }

    if (stats.topKLimited) {
      score += config.freqOverflowScore;
      reasons.push({
        code: "TOPK_LIMITED",
        weight: config.freqOverflowScore,
        message: "ユニーク値が多くTopK精度が低下",
      });
    }

    reasons.sort((a, b) => b.weight - a.weight);

    ranking.push({
      column,
      score: Math.min(100, score),
      reasons: reasons.slice(0, 3).map(({ code, message }) => ({ code, message })),
    });
  }

  ranking.sort((a, b) => b.score - a.score);
  return ranking;
}

function computeProfile(records, schema, topK) {
  const columnsSet = new Set();
  for (const record of records) {
    Object.keys(record || {}).forEach((key) => columnsSet.add(key));
  }
  const columns = [...columnsSet];
  const logicalTypes = detectLogicalTypes(schema);
  const statsMap = initColumnStats(columns);

  records.forEach((record, index) => {
    for (const column of columns) {
      const stats = statsMap[column];
      const value = record ? record[column] : undefined;

      if (value === null || value === undefined) {
        stats.nullCount += 1;
        continue;
      }

      stats.nonNullCount += 1;
      updateTypeCounts(stats, value);
      updateFrequency(stats, value);

      if (typeof value === "number" && Number.isFinite(value)) {
        stats.numericCount += 1;
        updateMinMax(stats, value);
        if (logicalTypes[column] === "timestamp-millis") {
          updateDateMinMax(stats, value);
        } else if (logicalTypes[column] === "timestamp-micros") {
          updateDateMinMax(stats, value / 1000);
        }
      }
    }

    if (index % 1000 === 0) {
      self.postMessage({
        type: "PROGRESS",
        payload: { processedRecords: index + 1, totalRecords: records.length },
      });
    }
  });

  const columnProfiles = {};
  for (const column of columns) {
    const stats = statsMap[column];
    const logicalType = logicalTypes[column];
    const typeHint = computeTypeHint(stats, logicalType);
    const nullRate = records.length ? stats.nullCount / records.length : 0;
    const nonNullCount = stats.nonNullCount;
    const numericRatio = nonNullCount ? stats.numericCount / nonNullCount : 0;
    const topKValues = computeTopK(stats, nonNullCount, topK);
    const top1Rate = topKValues[0] ? topKValues[0].rate : 0;
    const isNumericEligible = numericRatio >= NUMERIC_THRESHOLD;

    const profile = {
      typeHint,
      nullCount: stats.nullCount,
      nullRate,
      nonNullCount,
      topK: topKValues,
      topKLimited: stats.freqOverflow,
      top1Rate,
      numericRatio,
    };

    if (logicalType === "timestamp-millis" || logicalType === "timestamp-micros") {
      if (stats.dateMin !== null && stats.dateMax !== null && isNumericEligible) {
        profile.min = stats.dateMin;
        profile.max = stats.dateMax;
        profile.minDisplay = new Date(stats.dateMin).toISOString();
        profile.maxDisplay = new Date(stats.dateMax).toISOString();
      } else {
        profile.minMaxReason = numericRatio > 0 ? "mixedのため算出なし" : "数値列ではないため算出なし";
      }
    } else if (isNumericEligible && stats.numericMin !== null && stats.numericMax !== null) {
      profile.min = stats.numericMin;
      profile.max = stats.numericMax;
    } else if (nonNullCount > 0) {
      profile.minMaxReason = numericRatio > 0 ? "mixedのため算出なし" : "数値列ではないため算出なし";
    }

    profile.minMaxFlat =
      profile.min !== undefined && profile.max !== undefined && profile.min === profile.max;

    columnProfiles[column] = profile;
  }

  const suspiciousRanking = buildSuspiciousRanking(columnProfiles, SCORE_CONFIG);

  return {
    totalRecords: records.length,
    columns: columnProfiles,
    suspiciousRanking,
  };
}

self.onmessage = (event) => {
  const { type, payload } = event.data || {};
  if (type !== "START") return;
  const { records, schema, topK } = payload;

  try {
    const profile = computeProfile(records || [], schema || null, topK || 10);
    self.postMessage({ type: "RESULT", payload: profile });
  } catch (error) {
    self.postMessage({
      type: "ERROR",
      payload: { message: error?.message || "Profile計算に失敗しました" },
    });
  }
};
