"use strict";

const $ = (id) => document.getElementById(id);
const statusLabels = {
  pending: "В очереди", processing: "Обработка", retrying: "Повторная попытка",
  completed: "Готово", partial: "Частично готово", failed: "Ошибка", cancelled: "Отменено",
};
const operationLabels = { resize: "Resize", thumbnail: "Thumbnail", watermark: "Watermark" };
const terminal = new Set(["completed", "partial", "failed"]);
const images = new Map();
const cards = new Map();
const deleted = new Set();
let selectedFile = null;
let previewURL = "";
let fileReady = false;
let nextCursor = "";
let listLoading = false;
let pollTimer;
let polling = false;
let viewingID = null;
let viewingVersion = "original";
let deletingID = null;
let deleting = false;

function showMessage(id, message) {
  $(id).textContent = message;
  $(id).hidden = !message;
}

function notice(message, error = false) {
  $("notice").classList.toggle("error", error);
  showMessage("notice", message);
}

function sizeLabel(bytes) {
  return bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MiB` : `${Math.ceil(bytes / 1024)} KiB`;
}

function badge(status, prefix = "") {
  const span = document.createElement("span");
  span.className = "badge";
  span.dataset.status = status;
  span.textContent = `${prefix}${statusLabels[status] || status}`;
  return span;
}

async function api(path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.method === "POST" ? 120000 : 10000);
  try {
    const response = await fetch(path, { ...options, signal: controller.signal });
    const data = await response.json();
    if (!response.ok) {
      const error = new Error(data.error?.message || "Не удалось выполнить запрос.");
      error.status = response.status;
      throw error;
    }
    return data;
  } catch (error) {
    if (error.name === "AbortError") throw new Error(options.method === "POST"
      ? "Время ожидания истекло. Проверьте список перед повторной загрузкой."
      : "Время ожидания истекло. Повторите запрос.");
    if (error instanceof TypeError) throw new Error("Нет связи с сервером. Проверьте подключение и повторите попытку.");
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

for (const type of ["resize", "thumbnail"]) {
  $(type).addEventListener("change", () => {
    const enabled = $(type).checked;
    $(`${type}-params`).hidden = !enabled;
    for (const input of $(`${type}-params`).querySelectorAll("input")) input.disabled = !enabled;
    showMessage("upload-error", "");
  });
}

function clearFile(clearInput = true) {
  selectedFile = null;
  fileReady = false;
  if (previewURL) URL.revokeObjectURL(previewURL);
  previewURL = "";
  if (clearInput) $("file").value = "";
  $("file-preview").hidden = true;
  $("file-preview").removeAttribute("src");
  $("file-placeholder").hidden = false;
  $("file-info").textContent = "До 25 MP, не более 10 000 px по стороне.";
}

$("file").addEventListener("change", async () => {
  const file = $("file").files[0];
  clearFile(false);
  if (!file) return;
  selectedFile = file;
  showMessage("upload-error", "");
  if (file.size > 20 * 1048576 || file.size === 0) {
    showMessage("upload-error", "Выберите непустое изображение размером не более 20 MiB.");
    return;
  }
  try {
    const header = new Uint8Array(await file.slice(0, 8).arrayBuffer());
    if (selectedFile !== file) return;
    const jpeg = header[0] === 255 && header[1] === 216;
    const png = [137, 80, 78, 71, 13, 10, 26, 10].every((value, index) => header[index] === value);
    if (!jpeg && !png) throw new Error("Поддерживаются только JPEG и PNG.");
    const url = URL.createObjectURL(file);
    previewURL = url;
    const preview = new Image();
    preview.onload = () => {
      if (selectedFile !== file) return;
      const width = preview.naturalWidth;
      const height = preview.naturalHeight;
      if (width > 10000 || height > 10000 || width * height > 25000000) {
        showMessage("upload-error", "Изображение должно быть не более 25 MP и 10 000 px по стороне.");
        URL.revokeObjectURL(url);
        return;
      }
      fileReady = true;
      $("file-preview").src = url;
      $("file-preview").hidden = false;
      $("file-placeholder").hidden = true;
      $("file-info").textContent = `${file.name} · ${width} × ${height} · ${sizeLabel(file.size)}`;
    };
    preview.onerror = () => {
      if (selectedFile === file) showMessage("upload-error", "Не удалось прочитать изображение. Выберите другой файл.");
      URL.revokeObjectURL(url);
    };
    preview.src = url;
  } catch (error) {
    if (selectedFile === file) showMessage("upload-error", error.message);
  }
});

$("upload-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  showMessage("upload-error", "");
  if (!selectedFile || !fileReady) {
    showMessage("upload-error", "Выберите корректное JPEG или PNG и дождитесь предпросмотра.");
    return;
  }
  const operations = [];
  if ($("resize").checked) operations.push({ type: "resize", width: Number($("width").value), height: Number($("height").value) });
  if ($("thumbnail").checked) operations.push({ type: "thumbnail", size: Number($("size").value) });
  if ($("watermark").checked) operations.push({ type: "watermark" });
  if (!operations.length) {
    showMessage("upload-error", "Выберите хотя бы одно преобразование.");
    return;
  }
  const body = new FormData();
  body.append("file", selectedFile, selectedFile.name);
  body.append("operations", JSON.stringify(operations));
  $("upload-fields").disabled = true;
  $("submit").disabled = true;
  $("upload-form").setAttribute("aria-busy", "true");
  $("submit-label").textContent = "Загружаем изображение…";
  try {
    const image = await api("/upload", { method: "POST", body });
    updateImage(image, true);
    clearFile();
    showMessage("list-message", "");
    notice("Изображение загружено. Статусы операций обновляются автоматически.");
    schedulePoll();
  } catch (error) {
    showMessage("upload-error", error.message);
  } finally {
    $("upload-fields").disabled = false;
    $("submit").disabled = false;
    $("upload-form").removeAttribute("aria-busy");
    $("submit-label").textContent = "Начать обработку";
  }
});

function contentURL(image, version) {
  const base = `/image/${encodeURIComponent(image.id)}`;
  return version === "original" ? `${base}/original` : `${base}?operation=${encodeURIComponent(version)}`;
}

function updateImage(image, prepend = false) {
  if (deleted.has(image.id)) return;
  const previous = images.get(image.id);
  images.set(image.id, image);
  let card = cards.get(image.id);
  if (!card) {
    card = $("image-card").content.firstElementChild.cloneNode(true);
    cards.set(image.id, card);
    card.dataset.id = image.id;
    card.querySelector(".card-preview").addEventListener("click", () => openViewer(image.id));
    card.querySelector(".open").addEventListener("click", () => openViewer(image.id));
    card.querySelector(".remove").addEventListener("click", () => askDelete(image.id));
    const preview = card.querySelector("img");
    preview.addEventListener("error", () => {
      preview.hidden = true;
      card.querySelector(".preview-unavailable").hidden = false;
    });
    preview.src = contentURL(image, "original");
    if (prepend) $("images").prepend(card);
    else $("images").append(card);
  }
  if (JSON.stringify(previous) === JSON.stringify(image)) return;
  card.querySelector("h3").textContent = image.original_name;
  card.querySelector("h3").title = image.original_name;
  card.querySelector(".card-heading .badge").replaceWith(badge(image.status));
  card.querySelector(".card-meta").textContent = `${image.width} × ${image.height} · ${sizeLabel(image.size_bytes)} · ${image.content_type === "image/png" ? "PNG" : "JPEG"}`;
  card.querySelector(".job-chips").replaceChildren(...image.jobs.map((job) => badge(job.status, `${operationLabels[job.operation.type]} · `)));
  card.querySelector(".remove").setAttribute("aria-label", `Удалить ${image.original_name}`);
  if (viewingID === image.id) renderViewer();
  $("empty").hidden = true;
}

async function loadImages(reset = false) {
  if (listLoading) return;
  listLoading = true;
  $("refresh").disabled = true;
  $("load-more").disabled = true;
  showMessage("list-message", "Загружаем список…");
  const knownIDs = new Set(images.keys());
  try {
    const cursor = reset ? "" : nextCursor;
    const page = await api(`/images?limit=12${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
    if (reset) {
      // Keep uploads that finished while this request was in flight.
      const newest = [...images.values()].filter((image) => !knownIDs.has(image.id));
      images.clear();
      cards.clear();
      $("images").replaceChildren();
      for (const image of newest) updateImage(image);
    }
    for (const image of page.images) updateImage(image);
    nextCursor = page.next_cursor || "";
    $("load-more").hidden = !nextCursor;
    $("empty").hidden = images.size > 0;
    showMessage("list-message", "");
  } catch (error) {
    showMessage("list-message", `${error.message} Нажмите «Обновить», чтобы попробовать снова.`);
  } finally {
    listLoading = false;
    $("refresh").disabled = false;
    $("load-more").disabled = false;
    schedulePoll();
  }
}

function schedulePoll(delay = 2000) {
  clearTimeout(pollTimer);
  if (!polling && !document.hidden && [...images.values()].some((image) => !terminal.has(image.status))) {
    pollTimer = setTimeout(poll, delay);
  }
}

async function poll() {
  if (document.hidden) return;
  polling = true;
  let failed = false;
  const pending = [...images.values()].filter((image) => !terminal.has(image.status));
  await Promise.all(pending.map(async (image) => {
    try {
      const info = await api(`/image/${encodeURIComponent(image.id)}/status`);
      if (images.has(image.id)) updateImage(info);
    } catch (error) {
      if (error.status === 404) removeImage(image.id);
      else failed = true;
    }
  }));
  if (failed) showMessage("list-message", "Не удалось обновить статусы. Повторим запрос автоматически.");
  else if (!listLoading) showMessage("list-message", "");
  polling = false;
  schedulePoll(failed ? 5000 : 2000);
}

function openViewer(id) {
  viewingID = id;
  viewingVersion = "original";
  renderViewer();
  $("viewer").showModal();
}

function renderViewer() {
  const image = images.get(viewingID);
  if (!image) return;
  $("viewer-title").textContent = image.original_name;
  const versions = ["original", ...image.jobs.filter((job) => job.status === "completed").map((job) => job.operation.type)];
  const focusedVersion = document.activeElement?.dataset.version;
  $("versions").replaceChildren(...versions.map((version) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "button";
    button.dataset.version = version;
    button.textContent = version === "original" ? "Оригинал" : operationLabels[version];
    button.setAttribute("aria-pressed", String(viewingVersion === version));
    button.addEventListener("click", () => { viewingVersion = version; renderViewer(); });
    return button;
  }));
  if (focusedVersion) [...$("versions").children].find((button) => button.dataset.version === focusedVersion)?.focus();
  const job = image.jobs.find((entry) => entry.operation.type === viewingVersion);
  const url = contentURL(image, viewingVersion);
  if ($("viewer-image").getAttribute("src") !== url) {
    $("viewer-image").hidden = false;
    showMessage("viewer-message", "");
    $("viewer-image").src = url;
  }
  $("viewer-image").alt = `${image.original_name} — ${viewingVersion === "original" ? "оригинал" : operationLabels[viewingVersion]}`;
  $("viewer-meta").textContent = job
    ? `${job.result_width} × ${job.result_height} · ${sizeLabel(job.result_size_bytes)}`
    : `${image.width} × ${image.height} · ${sizeLabel(image.size_bytes)}`;
  $("download").href = url;
  const extension = image.content_type === "image/png" ? "png" : "jpg";
  const name = image.original_name.replace(/\.[^.]*$/, "").replace(/[\\/]/g, "_");
  $("download").download = `${name}-${viewingVersion}.${extension}`;
  $("job-details").replaceChildren(...image.jobs.map((entry) => {
    const li = document.createElement("li");
    const description = document.createElement("span");
    description.className = "job-description";
    description.textContent = operationLabels[entry.operation.type];
    const details = document.createElement("small");
    const op = entry.operation;
    details.textContent = op.type === "resize" ? `До ${op.width} × ${op.height} px` : op.type === "thumbnail" ? `${op.size} × ${op.size} px` : "ImageProcessor · правый нижний угол";
    if (entry.status === "failed") details.textContent += " · Не удалось обработать изображение";
    description.append(details);
    li.append(description, badge(entry.status));
    return li;
  }));
}

$("viewer-image").addEventListener("error", () => {
  $("viewer-image").hidden = true;
  showMessage("viewer-message", "Не удалось загрузить изображение. Закройте окно и попробуйте снова.");
});
$("close-viewer").addEventListener("click", () => $("viewer").close());
$("viewer").addEventListener("close", () => {
  viewingID = null;
  $("viewer-image").removeAttribute("src");
});

function askDelete(id) {
  deletingID = id;
  $("delete-name").textContent = images.get(id).original_name;
  showMessage("delete-error", "");
  $("delete-dialog").showModal();
  $("cancel-delete").focus();
}

function removeImage(id) {
  deleted.add(id);
  images.delete(id);
  cards.get(id)?.remove();
  cards.delete(id);
  if (viewingID === id) $("viewer").close();
  $("empty").hidden = images.size > 0 || Boolean(nextCursor);
}

$("cancel-delete").addEventListener("click", () => $("delete-dialog").close());
$("delete-dialog").addEventListener("cancel", (event) => { if (deleting) event.preventDefault(); });
$("confirm-delete").addEventListener("click", async () => {
  deleting = true;
  $("confirm-delete").disabled = true;
  $("cancel-delete").disabled = true;
  $("confirm-delete").textContent = "Удаляем…";
  showMessage("delete-error", "");
  try {
    await api(`/image/${encodeURIComponent(deletingID)}`, { method: "DELETE" });
    removeImage(deletingID);
    $("delete-dialog").close();
    notice("Изображение и его результаты удалены из списка.");
  } catch (error) {
    showMessage("delete-error", error.message);
  } finally {
    deleting = false;
    $("confirm-delete").disabled = false;
    $("cancel-delete").disabled = false;
    $("confirm-delete").textContent = "Удалить";
  }
});

$("refresh").addEventListener("click", () => loadImages(true));
$("load-more").addEventListener("click", () => loadImages());
document.addEventListener("visibilitychange", () => schedulePoll());
window.addEventListener("pagehide", () => clearTimeout(pollTimer));
window.addEventListener("pageshow", () => schedulePoll());
loadImages(true);
