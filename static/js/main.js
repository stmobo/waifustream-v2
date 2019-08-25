function createResultCard(data) {
    var id = data.img_id;

    var container = document.createElement('div');
    container.setAttribute('class', 'result-card');

    var img = document.createElement('img');
    img.setAttribute('class', 'result-card-image');
    img.setAttribute('src', 'api/image/' + id);
    img.setAttribute('alt', id);

    var label = document.createElement('span');
    label.setAttribute('class', 'result-card-label');
    label.innerText = data.source_site + '#' + id;

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

function doSearch() {
    fetchSearchResults('monika', 20, 0, null, 'explicit').then(function (results) {
        var cards = results.map(createResultCard);

        var container = document.getElementById("index-list-container");
        while (container.firstChild) {
            container.removeChild(container.firstChild);
        }

        cards.forEach(function (elem) {
            container.appendChild(elem);
        });
    })
}

document.addEventListener('DOMContentLoaded', function () {
    doSearch();
})