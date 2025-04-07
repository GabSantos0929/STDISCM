const EMAIL_KEY = 'email';
const TOKEN_KEY = 'token';
const ROLE_KEY = 'role';

export const storeAuthData = ({ email, token, role }) => {
    localStorage.setItem(EMAIL_KEY, email);
    localStorage.setItem(TOKEN_KEY, token);
    localStorage.setItem(ROLE_KEY, role);
};

export const getEmail = () => localStorage.getItem(EMAIL_KEY);
export const getToken = () => localStorage.getItem(TOKEN_KEY);
export const getRole = () => localStorage.getItem(ROLE_KEY);

export const clearAuthData = () => {
    localStorage.removeItem(EMAIL_KEY);
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(ROLE_KEY);
};
