export const handleLogout = (navigate) => {
  try {
    localStorage.removeItem('authToken');
    navigate('/');
  } catch (error) {
    alert("An error occurred during logout. Please try again.");
  }
};
