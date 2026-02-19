// This file is a part of Statrix
// Coding : Priyanshu Dey [@HellFireDevil18]

function paginationAction(type, action) {
    const currentPage = state.pagination[type]?.page || 1;
    const totalPages = state.pagination[type]?.totalPages || 1;

    switch(action) {
        case 'first':
            goToPage(type, 1);
            break;
        case 'prev':
            if (currentPage > 1) {
                goToPage(type, currentPage - 1);
            }
            break;
        case 'next':
            if (currentPage < totalPages) {
                goToPage(type, currentPage + 1);
            }
            break;
        case 'last':
            goToPage(type, totalPages);
            break;
    }
}

function goToPage(type, page) {
    page = parseInt(page);
    const totalPages = state.pagination[type]?.totalPages || 1;

    if (page < 1) page = 1;
    if (page > totalPages) page = totalPages;

    state.pagination[type].page = page;
    loadAllData();
}

document.addEventListener('click', (e) => {
    const isInsideDropdown = e.target.closest('.dropdown-toggle') || e.target.closest('.dropdown-menu');
    if (!isInsideDropdown) {
        document.querySelectorAll('.dropdown-menu').forEach(menu => {
            menu.classList.remove('show');
            menu.style.display = 'none';
        });
    }
});
