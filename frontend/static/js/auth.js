// This file is a part of Statrix
// Coding : Priyanshu Dey [@HellFireDevil18]

const API_BASE = window.location.origin;
const TOKEN_KEY = 'statrix_token';
const USER_KEY = 'statrix_user';

function setToken(token) {
    localStorage.setItem(TOKEN_KEY, token);
}

function getToken() {
    return localStorage.getItem(TOKEN_KEY);
}

function removeToken() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
}

function setUser(user) {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
}

function getUser() {
    const userStr = localStorage.getItem(USER_KEY);
    return userStr ? JSON.parse(userStr) : null;
}

function isAuthenticated() {
    return !!getToken();
}

async function apiRequest(endpoint, options = {}) {
    const token = getToken();

    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };

    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }

    const response = await fetch(`${API_BASE}${endpoint}`, {
        ...options,
        headers
    });

    if (response.status === 401) {
        removeToken();
        if (window.location.pathname !== '/edit') {
            window.location.href = '/edit';
        }
        throw new Error('Unauthorized');
    }

    return response;
}

async function handleLogin(event) {
    event.preventDefault();

    const form = event.target;
    const email = form.email.value;
    const password = form.password.value;
    const button = document.getElementById('login-button');
    const buttonText = button.querySelector('.btn-text');
    const buttonLoader = button.querySelector('.btn-loader');
    const errorDiv = document.getElementById('error-message');

    button.disabled = true;
    buttonText.style.display = 'none';
    buttonLoader.style.display = 'flex';
    errorDiv.style.display = 'none';

    try {
        const response = await apiRequest('/api/auth/login', {
            method: 'POST',
            body: JSON.stringify({ email, password })
        });

        if (response.ok) {
            const data = await response.json();
            setToken(data.access_token);

            const userResponse = await apiRequest('/api/auth/me');
            if (userResponse.ok) {
                const user = await userResponse.json();
                setUser(user);
            }

            window.location.href = '/edit/dashboard';
        } else {
            const error = await response.json();
            throw new Error(error.detail || 'Login failed');
        }
    } catch (error) {
        errorDiv.textContent = error.message || 'Invalid email or password';
        errorDiv.style.display = 'block';

        button.disabled = false;
        buttonText.style.display = 'flex';
        buttonLoader.style.display = 'none';
    }
}

async function handleLogout() {
    removeToken();
    window.location.href = '/edit';
}

function checkAuth() {
    if (!isAuthenticated()) {
        window.location.href = '/edit';
        return false;
    }
    return true;
}

document.addEventListener('DOMContentLoaded', () => {
    const loginForm = document.getElementById('login-form');
    if (loginForm) {
        loginForm.addEventListener('submit', handleLogin);
    }

    if (window.location.pathname.startsWith('/edit/dashboard') && !isAuthenticated()) {
        window.location.href = '/edit';
    }

    const logoutButton = document.getElementById('logout-button');
    if (logoutButton) {
        logoutButton.addEventListener('click', handleLogout);
    }
});
