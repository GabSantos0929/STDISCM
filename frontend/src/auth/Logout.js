export const handleLogout = (navigate) => {
    localStorage.removeItem('authToken');
    navigate('/');
};
