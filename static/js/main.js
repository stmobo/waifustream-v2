var currentPage = 0;
var noMoreResults = false;

var currentSearchCharacter = '';
var currentSearchTags = '';
var currentSearchRating = '';

function createResultCard(data) {
    var id = data.img_id;

    var container = document.createElement('div');
    container.setAttribute('class', 'result-card');

    var img = document.createElement('img');
    img.setAttribute('class', 'result-card-image');
    img.setAttribute('src', data.cache_path);
    img.setAttribute('alt', id);

    var label = document.createElement('span');
    label.setAttribute('class', 'result-card-label');
    label.innerText = data.source_site + '#' + data.source_id;

    container.appendChild(img);
    container.appendChild(label);

    return container;
}

function fetchSearchResults(character, count, page, tags, rating) {
    var url = 'api/characters/' + encodeURIComponent(character);

    url += '?count=' + count;
    url += '&page=' + page;

    if (rating) {
        url += '&rating=' + encodeURIComponent(rating);
    }

    if (tags) {
        tags.forEach(function (t) {
            t = t.trim();
            if (t.length > 0) url += '&tag=' + encodeURIComponent(t);
        });
    }

    return fetch(url).then((resp) => resp.json());
}

function clearResultsList() {
    var container = document.getElementById("index-list-container");
    while (container.firstChild) {
        container.removeChild(container.firstChild);
    }
}

function doSearch() {
    if (!currentSearchCharacter) return;

    fetchSearchResults(currentSearchCharacter, 20, currentPage, currentSearchTags, currentSearchRating).then(function (results) {
        if (results.length === 0) {
            noMoreResults = true;
            return;
        }

        noMoreResults = false;
        var cards = results.map(createResultCard);

        var container = document.getElementById("index-list-container");
        cards.forEach(function (elem) {
            container.appendChild(elem);
        });
    })
}

window.onscroll = function (ev) {
    if ((window.innerHeight + window.pageYOffset) >= document.body.offsetHeight - 2) {
        if (noMoreResults) return;

        currentPage += 1;
        doSearch();
    }
};

function startNewSearch() {
    clearResultsList();

    currentPage = 0;
    noMoreResults = false;

    currentSearchCharacter = $('#index-filter-character').val();
    currentSearchRating = $('input[name=index-filter-rating]:checked', '#index-filter-settings').val();
    currentSearchTags = $('#index-filter-tags').val().split(' ');

    doSearch();
}

document.addEventListener('DOMContentLoaded', function () {
    startNewSearch();

    fetch('api/characters')
        .then((resp) => resp.json())
        .then(function (characters) {
            var opts = characters.map(function (character) {
                var opt = document.createElement('option');

                opt.setAttribute('value', character);
                opt.innerText = character.substring(0, 1).toUpperCase() + character.substring(1).toLowerCase();

                return opt;
            });

            $("#index-filter-character").append(opts);
        });
})