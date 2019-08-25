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
            url += '&tag=' + encodeURIComponent(t);
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

var currentPage = 0;
var noMoreResults = false;

function doSearch() {
    fetchSearchResults('monika', 20, currentPage, null, 'explicit').then(function (results) {
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
        currentPage += 1;
        doSearch();
    }
};

document.addEventListener('DOMContentLoaded', function () {
    doSearch();
})