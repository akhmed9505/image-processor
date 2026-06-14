// DOM elements
const imageFileInput = document.getElementById('imageFile');
const fileText = document.getElementById('fileText');
const imagePreview = document.getElementById('imagePreview');
const previewImg = document.getElementById('previewImg');
const taskSelect = document.getElementById('taskSelect');
const watermarkGroup = document.getElementById('watermarkGroup');
const watermarkText = document.getElementById('watermarkText');
const resizeGroup = document.getElementById('resizeGroup');
const widthInput = document.getElementById('width');
const heightInput = document.getElementById('height');
const contentTypeSelect = document.getElementById('contentType');
const submitBtn = document.getElementById('submitBtn');
const resultSection = document.getElementById('resultSection');
const resultContent = document.getElementById('resultContent');
const loading = document.getElementById('loading');

// New elements for image management
const imageIdInput = document.getElementById('imageId');
const getImageBtn = document.getElementById('getImageBtn');
const deleteImageBtn = document.getElementById('deleteImageBtn');
const fetchedImageSection = document.getElementById('fetchedImageSection');
const fetchedImageContainer = document.getElementById('fetchedImageContainer');

// File upload handling
imageFileInput.addEventListener('change', function(e) {
    const file = e.target.files[0];
    if (file) {
        fileText.textContent = file.name;
        submitBtn.disabled = false;

        // Show image preview
        const reader = new FileReader();
        reader.onload = function(e) {
            previewImg.src = e.target.result;
            imagePreview.style.display = 'block';
        };
        reader.readAsDataURL(file);

        // Set content type based on file extension
        const fileName = file.name.toLowerCase();
        if (fileName.endsWith('.jpg') || fileName.endsWith('.jpeg')) {
            contentTypeSelect.value = 'image/jpeg';
        } else if (fileName.endsWith('.png')) {
            contentTypeSelect.value = 'image/png';
        } else if (fileName.endsWith('.gif')) {
            contentTypeSelect.value = 'image/gif';
        }
    } else {
        fileText.textContent = 'Файл не выбран';
        imagePreview.style.display = 'none';
        submitBtn.disabled = true;
    }
});

// Task selection handling
taskSelect.addEventListener('change', function(e) {
    const task = e.target.value;

    // Hide all task-specific fields
    watermarkGroup.style.display = 'none';
    resizeGroup.style.display = 'none';

    // Show relevant fields based on task
    switch(task) {
        case 'watermark':
            watermarkGroup.style.display = 'block';
            break;
        case 'resize':
            resizeGroup.style.display = 'block';
            break;
        case 'miniature generating':
            // No additional fields needed for thumbnail generation
            break;
    }
});

// Form validation
function validateForm() {
    const file = imageFileInput.files[0];
    const task = taskSelect.value;
    const contentType = contentTypeSelect.value;

    if (!file || !task || !contentType) {
        return false;
    }

    if (task === 'resize') {
        const width = widthInput.value;
        const height = heightInput.value;
        if (!width || !height || width <= 0 || height <= 0) {
            return false;
        }
    }

    return true;
}

// Update submit button state
function updateSubmitButton() {
    submitBtn.disabled = !validateForm();
}

// Add event listeners for form validation
taskSelect.addEventListener('change', updateSubmitButton);
contentTypeSelect.addEventListener('change', updateSubmitButton);
widthInput.addEventListener('input', updateSubmitButton);
heightInput.addEventListener('input', updateSubmitButton);

// Submit form
submitBtn.addEventListener('click', async function() {
    if (!validateForm()) {
        showResult('Пожалуйста, заполните все обязательные поля', 'error');
        return;
    }

    const file = imageFileInput.files[0];
    const task = taskSelect.value;
    const contentType = contentTypeSelect.value;
    const watermark = watermarkText.value;

    // Prepare metadata
    const metadata = {
        content_type: contentType,
        task: task,
        watermark_string: watermark,
        resize: {
            width: parseInt(widthInput.value) || 0,
            height: parseInt(heightInput.value) || 0
        }
    };

    // Create FormData
    const formData = new FormData();
    formData.append('image', file);
    formData.append('metadata', JSON.stringify(metadata));

    // Show loading
    loading.style.display = 'flex';

    try {
        const response = await fetch('/upload', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            const data = await response.json();
            showResult(`Изображение успешно отправлено на обработку! ID: ${data.id}`, 'success');
            // Set the ID in the input field for immediate use
            imageIdInput.value = data.id;
        } else {
            const errorData = await response.json();
            showResult(`Ошибка: ${errorData.error || 'Неизвестная ошибка'}`, 'error');
        }
    } catch (error) {
        showResult(`Ошибка сети: ${error.message}`, 'error');
    } finally {
        loading.style.display = 'none';
    }
});

// Get image by ID
getImageBtn.addEventListener('click', async function() {
    const imageId = imageIdInput.value.trim();

    if (!imageId) {
        showResult('Пожалуйста, введите ID изображения', 'error');
        return;
    }

    // Show loading
    loading.style.display = 'flex';

    try {
        const response = await fetch(`/image/${imageId}`, {
            method: 'GET'
        });

        if (response.ok) {
            // Create blob URL for the image
            const blob = await response.blob();
            const imageUrl = URL.createObjectURL(blob);

            // Display the image
            fetchedImageContainer.innerHTML = `
                <img src="${imageUrl}" alt="Fetched Image" style="max-width: 100%; border-radius: 8px; box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);">
                <div class="fetched-image-info">
                    <strong>ID:</strong> ${imageId}
                </div>
            `;
            fetchedImageSection.style.display = 'block';
        } else if (response.status === 200) {
            const data = await response.json();
            showResult(`Изображение в обработке: ${data.status}`, 'success');
        } else {
            const errorData = await response.json();
            showResult(`Ошибка: ${errorData.error || 'Неизвестная ошибка'}`, 'error');
        }
    } catch (error) {
        showResult(`Ошибка сети: ${error.message}`, 'error');
    } finally {
        loading.style.display = 'none';
    }
});

// Delete image by ID
deleteImageBtn.addEventListener('click', async function() {
    const imageId = imageIdInput.value.trim();

    if (!imageId) {
        showResult('Пожалуйста, введите ID изображения', 'error');
        return;
    }

    if (!confirm(`Вы уверены, что хотите удалить изображение с ID: ${imageId}?`)) {
        return;
    }

    // Show loading
    loading.style.display = 'flex';

    try {
        const response = await fetch(`/image/${imageId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            showResult(`Изображение с ID ${imageId} успешно удалено!`, 'success');
            // Clear the fetched image section
            fetchedImageSection.style.display = 'none';
            fetchedImageContainer.innerHTML = '';
        } else {
            const errorData = await response.json();
            showResult(`Ошибка при удалении: ${errorData.error || 'Неизвестная ошибка'}`, 'error');
        }
    } catch (error) {
        showResult(`Ошибка сети: ${error.message}`, 'error');
    } finally {
        loading.style.display = 'none';
    }
});

// Show result message
function showResult(message, type) {
    resultContent.innerHTML = `<div class="${type}">${message}</div>`;
    resultSection.style.display = 'block';

    // Don't auto-hide success messages with ID
    if (type === 'error') {
        setTimeout(() => {
            resultSection.style.display = 'none';
        }, 5000);
    }
}

// Initialize form state
updateSubmitButton();
