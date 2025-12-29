//
// Marketplace
//

var ROLL20 = ROLL20 || {};

(function ($) {

	// USE STRICT
	"use strict";

	ROLL20.initialize = {

		init: function () {

		}

	};

	ROLL20.marketplace = {

		init: function () {

			ROLL20.marketplace.filtersPanel();
			ROLL20.marketplace.searchInit();
			ROLL20.marketplace.wishlistStar();

		},

		filtersPanel: function () {
			$(".marketplace-search__filter-btn, .marketplace-filter-panel__header-close, .body-overlay").off('click').on('click', function () {
				$('body').toggleClass("marketplace-filter-panel-open overflow-hidden overlay-visible");
				return false;
			});
		},

		wishlistStar: function () {
			var $card = $(".card"),
				actions = '.card-product-actions';

			$('[data-toggle="tooltip"]').tooltip();

			$('[data-toggle="tooltip"]').click(function () {
				$('[data-toggle="tooltip"]').tooltip("hide");
			});

			if ($(window).width() > 992) {
				if ($card.length && $(actions).length) {
					$card.on({
						'mouseleave': function () {
							$('.wishlist-icon').tooltip('hide');
							$('.wishlist-icon').dropdown('hide');
						}
					})
				}
			}
		},

		searchInit: function () {
			var $selectizeData = $authorlist;
			var $selectizeDataItems = $selectizeData.map(function (x) { return { item: x, term: `::author::${x}`, category: 'author', label: "Author" }; });
			var capitalize = function (string) {
				return string.split(" ").map(function (x) { return x.substr(0, 1).toUpperCase() + x.substr(1) }).join(" ");
			};
			var initial = { keywords: "", filters: {} };
			var debounce = null;
			window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function (m, key, value) {
				if (["keywords", "page", "sortby"].includes(key)) {
					initial[key] = decodeURI(value).replace(/%26/g, "&");
				} else {
					initial.filters[key] = decodeURI(value).replace(/%26/g, "&").split("|");
				}
			});
			_.each($cat, function (v1, k1) {
				$selectizeDataItems.push({ item: k1, term: `::category::itemtype:${k1}`, category: "category", label: "Type" });
				_.each(v1, function (v2, k2) {
					$selectizeDataItems.push({ item: k2, term: `::category::${k1}:${k2}`, category: "category", label: k1 });
					_.each(v2, function (v3, k3) {
						$selectizeDataItems.push({ item: k3, term: `::category::${k2}:${k3}`, category: "category", label: k2 });
						_.each(v3, function (v4, k4) {
							$selectizeDataItems.push({ item: k4, term: `::category::${k3}:${k4}`, category: "category", label: k3 });
						});
					})
				});
			});
			_.each($tags, function (v1, k1) {
				_.each(v1, function (v2, k2) {
					$selectizeDataItems.push({ item: `${k1}: ${k2}`, term: `::tag::${k1}:${k2}`, category: "tag", label: k1 });
					_.each(v2, function (v3, k3) {
						$selectizeDataItems.push({ item: `${k2}: ${k3}`, term: `::tag::${k2}:${k3}`, category: "tag", label: k2 });
					})
				});
			});

			var searchInput = $('[data-toggle="marketplace-selectize"]').selectize({
				openOnFocus: false,
				create: function (input) {
					if (typeof input == "string") {
						let keyArray = _.uniq(input.split(" "));
						let thisInput = keyArray.pop();
						_.each(keyArray, async function (v) {
							ROLL20.marketplace.searchInput.createItem(v, false);
						});
						return { item: thisInput, term: `::keyword::${thisInput}`, category: "keyword", label: "Keyword" };
					} else {
						let term = input.length > 2 ? `::${input[2]}::${input[1]}:${input[0]}` : `::${input[1]}::${input[0]}`;
						let type = input.length > 2 ? input[2] : input[1];
						return { item: input[0], term: term, category: type, label: input[1] };
					}
				},
				options: $selectizeDataItems,
				closeAfterSelect: true,
				labelField: "item",
				valueField: "term",
				searchField: "item",
				addPrecedence: true,
				optgroups: [
					{ value: 'author', label: 'Filter by Authors' },
					{ value: 'category', label: 'Filter by Category' },
					{ value: 'tag', label: 'Filter by Tags' }
				],
				optgroupField: 'category',
				onDelete: function (values) {
					_.each(values, function (value) {
						let category = value.split("::")[1];
						if (category == "category") {
							let parent = value.split("::")[2].split(":")[0];
							let filter = value.split("::")[2].split(":")[1];
							$(`input[data-parent="${parent}"][data-filter="${filter}"]`).prop("checked", false);
						}
					});
				},
				onItemAdd(value) {
					let category = value.split("::")[1];
					if (category == "category") {
						ROLL20.marketplace.checkBoxes(value.split("::")[2].split(":")[0], value.split("::")[2].split(":")[1]);
					}
				},
				onChange: function () {
					if (!ROLL20.marketplace.ignoreChange) {
						clearTimeout(debounce);
						debounce = setTimeout(function () {
							let terms = ROLL20.marketplace.searchTerms();
							ROLL20.marketplace.addHistory(terms);
							ROLL20.marketplace.refreshResults(terms);
						}, 100);
					}
				},
				render: {
					item: function (data, escape) {
						let result = `<div class="custom-tag" data-term="${data.term}" data-type="${data.category}" data-label="${data.label}">`;
						result += `<span class="custom-tag__title">${capitalize(data.label.replace("itemtype", "Type"))}</span><br />`;
						result += `<span class="custom-tag__name">${escape(data.item)}</span></div>`;
						return result;
					},
					optgroup_header: function (data, escape) {
						return `<div class="optgroup-header label">${escape(data.label)}</div>`;
					}
				},
			});

			ROLL20.marketplace.searchInput = searchInput[0].selectize;
			ROLL20.marketplace.addSearchTerms(initial);
			ROLL20.marketplace.refreshResults(initial);
		},

		searchTerms: function () {
			let terms = { keywords: [], filters: {} };
			let sortby = $("#input_sort").val();
			$(".selectize-input.items .custom-tag").each(function () {
				let type = $(this).data("type");
				let value = $(this).data("term").replace(/::.+::/g, "");
				if (type == "keyword") {
					terms.keywords.push(value);
				} else {
					terms.filters[type] = terms.filters[type] || [];
					terms.filters[type].push(value);
				}
			});
			terms.keywords = terms.keywords.reverse().join(" ");
			if (sortby != ROLL20.marketplace.defaultSort) terms.sortby = sortby;
			return terms;
		},

		refreshResults: function (terms = {}, scroll) {
			ROLL20.marketplace.updateFilterPanel();
			if (ROLL20.marketplace.homePage) return;
			$.ajax({
				url: '/browse/search',
				method: 'POST',
				dataType: 'json',
				data: terms
			}).done(function (data) {
				$(".marketplace-content__wrapper").html("");
				_.each(data.results, function (result) {
					const avatarImage = window.d20ext.utils.getS3Endpoint(result.avatar);

					let thisitem = `<div class="card card-product m-0">
						<div class="card-image p-0">
							<img src="${avatarImage}" alt="${result.name}" class="card-img-top img-center img-fluid">
						</div>
						<div class="card-body py-3 px-4">
							<h5 class="card-subtitle h6 mb-0 text-muted text-truncate marketplace-item-subtitle">${result.author}</h5>
							<h6 class="card-title h5 text-truncate">${result.name}</h6>
							<div class="browse-price-row">`
					if (result.discount > 0){
						thisitem +=	`<div class="card-price-box">
										<span class="card-price dynamic-price" data-itemid=${result.id}>${result.cost}</span>
										<span class="item-currency"> USD</span>
									</div>`
						thisitem +=	`<p class="browse-discount browse-discount-hidden">-${result.discount}%</p>`
					} else if (result.sale_discount > 0) {
						thisitem += `
							<div class="card-price-box">
								<span class="card-price sale-price" data-itemid=${result.id}>${result.cost}</span>
								<span class="item-currency sale-price">USD</span>
							</div>
							<p class="browse-discount">-${result.sale_discount}%</p>`
					} else if (!result.has_external_url) {
						thisitem +=	`<div class="card-price-box">
										<span class="card-price"><strong>${result.cost}</strong></span>
										<span class="item-currency"><strong> USD</strong></span>
									</div>`
					}
					thisitem +=	`
							</div>
							<a href="${result.url}" class="stretched-link"><span class="sr-only">${result.name}</span></a>
						</div>`;
					if (data.loggedin) {
						thisitem += `<div class="actions card-product-actions">
							<div class="dropdown" data-toggle="dropdown">
								<button type="button" class="action-item wishlist-icon" data-display="static" aria-haspopup="true" aria-expanded="false" data-toggle="tooltip" data-original-title="${translations.add_to_wishlist}">
									<i class="fas fa-star"></i>
								</button>
								<div class="dropdown-menu dropdown-menu wishlist-dropdown-menu" data-itemid="${result.id}">
									<div class="scrollable-menu">`;
						_.each(data.wishlists, function (list) {
							let included = list.items.includes(result.id);
							thisitem += `<div class="dropdown-item wishlist-dropdown-item wl ${included ? `disabled" aria-disabled="true"` : `"`} data-listid="${list.id}">
								<a href="javascript:void(0)" tabindex="-1" class="disabled text-muted" aria-disabled="true">
									<div class="d-flex justify-content-between flex-column flex-xxl-row">
										<div class="mr-2">
											<div class="wishlist-dropdown-item__title d-block">${list.name}</div>
											<div class="wishlist-dropdown-item__privacy small">${list.public ? translations.wishlist_public : translations.wishlist_private}</div>
										</div>
										<div>
											<div class="wishlist-dropdown-item__items text-xxl-right small">${translations.wishlist_items.replace("{{0}}", list.items.length)}</div>
										</div>
									</div>
									<div class="wishlist-dropdown-item__in-wishlist small text-tertiary">${translations.wishlist_in_list}</div>
								</a>
							</div>`;
						});
						thisitem += `<div class="dropdown-item wishlist-dropdown-item text-muted small empty-message">${translations.wishlist_none}</div>
									</div>
									<div class="dropdown-divider"></div>
									<div class="dropdown-item">
										<a href="#" class="createwishlist">${translations.wishlists_create}</a>
									</div>
								</div>
							</div>
						</div>`;
					}
					thisitem += `<div class="card-footer py-2 px-4">
							<ul class="card-tags list-inline text-sm m-0">`;
					_.each(result.categories, function (category) {
						thisitem += `<li class="list-inline-item marketplace-tag">${category}</li>`;
					});
					thisitem += `</ul></div></div>`;
					$("#marketplace-content .marketplace-content__wrapper").append(thisitem);
				});
				$("#resultnum").html(data.itemcount);

				// Pagination
				ROLL20.marketplace.currentPage = data.page;
				$(".pagination li").removeClass("disabled");
				if (data.page == 1) {
					$(".pagination #firstPageButton").addClass("disabled");
					$(".pagination #previousPageButton").addClass("disabled");
				} else if (data.page == data.totalpages) {
					$(".pagination #lastPageButton").addClass("disabled");
					$(".pagination #nextPageButton").addClass("disabled");
				}

				if (data.totalpages == 1) {
					$(".pagination #firstPageButton").addClass("disabled");
					$(".pagination #previousPageButton").addClass("disabled");
					$(".pagination #lastPageButton").addClass("disabled");
					$(".pagination #nextPageButton").addClass("disabled");
				}

				$(".pagination #nextPageButton").data("page", data.page + 1);
				$(".pagination #previousPageButton").data("page", data.page - 1);
				$(".pagination #lastPageButton").data("page", data.totalpages);

				$(".pagination #currentpage").html(data.page);
				$(".pagination #lastpage").html(data.totalpages);

				//sort by
				$("#input_sort").val(data.sortby);
				ROLL20.marketplace.defaultSort = data.keywords === "" ? "new" : "";

				//scroll to top
				if (scroll) $("html,body").animate({ scrollTop: $("#marketplace-search").offset().top }, "fast");

				//add listeners for wishlist
				$(".wishlist-dropdown-menu").on("click", ".wishlist-dropdown-item.wl", function () {
					let listid = $(this).data("listid");
					let itemid = $(this).parent().parent().data("itemid");
					ROLL20.marketplace.addItemToWishlist(listid, itemid);
				});

				$(".wishlist-dropdown-menu").on("click", "a.createwishlist", function () {
					let itemid = $(this).parents("[data-itemid]").data("itemid");
					if (itemid) {
						$("#createNewWishlistModal input[name=itemid]").val(itemid);
					}
					$("#createNewWishlistModal").modal();
				});
				ROLL20.marketplace.renderDynamicPrice();
			});
		},

		renderDynamicPrice: () => {
			$('.dynamic-price').each((i, e) => {
				const priceSpan = $(e);
				const bundleID = priceSpan.data('itemid');
				const requestData = {bundle_id: bundleID}
				$.ajax({
					url: `/api/bundleprice/${bundleID}`,
					method: "POST",
					dataType: 'json',
					data: requestData,
					success: (response) => {
						const price = response.price;
						if (price !== 'FREE'){
							$(e).text(price);
						}
						$(e).parent().parent().find('.browse-discount').removeClass('browse-discount-hidden');
						$(e).parent().find('.dynamic-price').addClass('browse-bundle-price');
						$(e).parent().find('.item-currency').addClass('browse-bundle-price');
					},
					error: (response) => {
						console.log("There was an error retrieving user price.");
					}
				});
			});
		},

		addHistory: function (terms = {}) {
			let urlvars = [];
			if (terms.keywords) urlvars.push(`keywords=${terms.keywords}`);
			if (terms.filters) {
				_.each(terms.filters, function (v, k) {
					urlvars.push(`${k}=${v.join("|").replace(/&/g, "%26")}`);
				});
			};
			if (terms.page && terms.page != 1) urlvars.push(`page=${terms.page}`);
			if (terms.sortby) urlvars.push(`sortby=${terms.sortby}`);
			let url = urlvars.length > 0 ? `search?${urlvars.join("&")}` : `search`;
			if (ROLL20.marketplace.homePage) {
				ROLL20.marketplace.searchUrl = url;
				return;
			}
			history.pushState(terms, "", url);
		},

		addSearchTerms: function (terms = {}) {
			terms = terms || {}
			if (terms.filters) {
				_.each(terms.filters, function (v, k) {
					_.each(v, function (val) {
						ROLL20.marketplace.searchInput.addItem(`::${k}::${val}`, true);
						ROLL20.marketplace.checkBoxes(val.split(":")[0], val.split(":")[1]);
					});
				});
			}
			if (terms.keywords) {
				ROLL20.marketplace.ignoreChange = true;
				ROLL20.marketplace.searchInput.createItem(terms.keywords, false);
				ROLL20.marketplace.ignoreChange = false;
			}
		},

		checkBoxes: function (parent, filter) {
			let $check = $(`input[data-parent="${parent}"][data-filter="${filter}"]`);
			$check.prop("checked", true);
			$check.parents(".collapse").collapse("show");
		},

		updateFilterPanel: function () {
			let $div = $(".marketplace-filter-panel__active-filters");
			$div.html("");
			$(".marketplace-filter-panel__active-filters .active-filters__item-close").off();
			$("#marketplace-filter-panel__active-filters-clear .number, .marketplace-search__filter-btn .number").html($(".selectize-input.items .custom-tag").length);
			$(".selectize-input.items .custom-tag").each(function () {
				let type = $(this).data("label");
				let value = $(this).data("term");
				$div.append(`<span class="active-filters__item">
					<span class="active-filters__item-row d-flex justify-content-between align-items-center">
						<span class="active-filters__item-col">
							<span class="active-filters__item-category">${type}</span>
							<span class="active-filters__item-subcategory">${_.last(value.split(":"))}</span>
						</span>
						<span class="active-filters__item-col">
							<span class="active-filters__item-close" data-value="${value}"><i class="fal fa-times"></i></span>
						</span>
					</span>
				</span>`);
				if ($(this).find(".remove").length == 0) {
					$(this).append(`<a href="javascript:void(0)" class="remove" tabindex="-1" title="Remove">x</a>`);
				}
				$(this).off().on("click", ".remove", function () {
					let category = value.split("::")[1];
					if (category == "category") {
						let parent = value.split("::")[2].split(":")[0];
						let filter = value.split("::")[2].split(":")[1];
						$(`input[data-parent="${parent}"][data-filter="${filter}"]`).prop("checked", false);
					}
					ROLL20.marketplace.searchInput.removeItem(value, false)
				})
			});
			$(".marketplace-filter-panel__active-filters .active-filters__item-close").on("click", function () {
				let value = $(this).data("value");
				let category = value.split("::")[1];
				if (category == "category") {
					let parent = value.split("::")[2].split(":")[0];
					let filter = value.split("::")[2].split(":")[1];
					$(`input[data-parent="${parent}"][data-filter="${filter}"]`).prop("checked", false);
				}
				ROLL20.marketplace.searchInput.removeItem(value, false);
			});
			// Update filter tag collapser here
			ROLL20.marketplace.toggleFilterTags();
		},

		toggleFilterTags: function () {
			var $marketplaceSelectizeFilterTags = $('div.selectize-input.items > div.custom-tag');

			if ($marketplaceSelectizeFilterTags.length > 4) {
				$("div.selectize-input.items").addClass("hide-tags");
				if ($("div.selectize-input.items").find(".selectize-input__toggler").length < 1) {
					$("div.selectize-input.items").prepend(`<a class="btn btn-sm btn-secondary selectize-input__toggler collapsed" data-toggle="collapse" href="#marketplace-search__filter-container" role="button" aria-expanded="false" aria-controls="marketplace-search__filter-container">Show Filters (<span class="number"></span>)</a>`);
				}
				$(".selectize-input__toggler .number").html($(".selectize-input.items .custom-tag").length);
				$("#marketplace-search__filter-container").html($("div.selectize-input.items > div").clone());

				$("#marketplace-search__filter-container .custom-tag .remove").off().on("click", function () {
					let value = $(this).parent().data("value");
					let category = value.split("::")[1];
					if (category == "category") {
						let parent = value.split("::")[2].split(":")[0];
						let filter = value.split("::")[2].split(":")[1];
						$(`input[data-parent="${parent}"][data-filter="${filter}"]`).prop("checked", false);
					}
					ROLL20.marketplace.searchInput.removeItem(value, false);
				});

				$('.selectize-input__toggler').off().on('click', function () {
					if ($(this).hasClass("collapsed")) {
						$(this).html("Hide Filters");
					} else {
						$(this).html(`Show Filters (<span class="number">${$(".selectize-input.items .custom-tag").length}</span>)`);
					}
				});
			} else {
				$(".selectize-input__toggler").remove();
				$("div.selectize-input.items").removeClass("hide-tags");
				$("#marketplace-search__filter-container").html("");
			}
		},

		addItemToWishlist: function (listid, itemid) {
			$.ajax({
				url: `/wishlist/additem`,
				data: {
					itemid: itemid,
					listid: listid
				},
				type: 'post',
				dataType: 'json'
			}).done(function (data) {
				let list = _.where(data, { id: listid })[0];
				if (list != undefined) {
					let itemcount = translations.wishlist_items.replace("{{0}}", list.items.length);
					$(`.wishlist-dropdown-item.wl[data-listid=${listid}]`).find(".wishlist-dropdown-item__items").html(itemcount);
				}
				$(`.wishlist-dropdown-menu[data-itemid=${itemid}] .wishlist-dropdown-item.wl[data-listid=${listid}]`).addClass("disabled").attr("aria-disabled", "true");

				ROLL20.widget.alertMessage(translations.wishlist_alert_add_item);

			});
		},

		ignoreChange: false,

		currentPage: 1,

		searchUrl: "search",

		defaultSort: "new"

	};

	ROLL20.widget = {

		init: function () {

			ROLL20.widget.carousel();

		},

		carousel: function () {

			if (!$().owlCarousel) {
				console.log('carousel: Owl Carousel not Defined.');
				return true;
			}

			var $carousel = $('.carousel-widget:not(.customjs)');
			if ($carousel.length < 1) { return true; }

			$carousel.each(function () {
				var element = $(this),
					elementItems = element.attr('data-items'),
					elementItemsXXXl = element.attr('data-items-xxxl'),
					elementItemsXXl = element.attr('data-items-xxl'),
					elementItemsXl = element.attr('data-items-xl'),
					elementItemsLg = element.attr('data-items-lg'),
					elementItemsMd = element.attr('data-items-md'),
					elementItemsSm = element.attr('data-items-sm'),
					elementItemsXs = element.attr('data-items-xs'),
					elementLoop = element.attr('data-loop'),
					elementAutoPlay = element.attr('data-autoplay'),
					elementSpeed = element.attr('data-speed'),
					elementAnimateIn = element.attr('data-animate-in'),
					elementAnimateOut = element.attr('data-animate-out'),
					elementNav = element.attr('data-nav'),
					elementPagi = element.attr('data-pagi'),
					elementMargin = element.attr('data-margin'),
					elementStage = element.attr('data-stage-padding'),
					elementMerge = element.attr('data-merge'),
					elementStart = element.attr('data-start'),
					elementRewind = element.attr('data-rewind'),
					elementSlideBy = element.attr('data-slideby'),
					elementCenter = element.attr('data-center'),
					elementLazy = element.attr('data-lazyload'),
					elementVideo = element.attr('data-video'),
					elementRTL = element.attr('data-rtl'),
					elementAutoPlayTime = 5000,
					elementAutoPlayHoverPause = true;

				if (!elementItems) { elementItems = 4; }
				if (!elementItemsXXXl) { elementItemsXXXl = Number(elementItems); }
				if (!elementItemsXXl) { elementItemsXXl = Number(elementItemsXXXl); }
				if (!elementItemsXl) { elementItemsXl = Number(elementItemsXXl); }
				if (!elementItemsLg) { elementItemsLg = Number(elementItemsXl); }
				if (!elementItemsMd) { elementItemsMd = Number(elementItemsLg); }
				if (!elementItemsSm) { elementItemsSm = Number(elementItemsMd); }
				if (!elementItemsXs) { elementItemsXs = Number(elementItemsSm); }
				if (!elementSpeed) { elementSpeed = 250; }
				if (!elementMargin) { elementMargin = 20; }
				if (!elementStage) { elementStage = 0; }
				if (!elementStart) { elementStart = 0; }

				if (!elementSlideBy) { elementSlideBy = 1; }
				if (elementSlideBy == 'page') {
					elementSlideBy = 'page';
				} else {
					elementSlideBy = Number(elementSlideBy);
				}

				if (elementLoop == 'true') { elementLoop = true; } else { elementLoop = false; }
				if (!elementAutoPlay) {
					elementAutoPlay = false;
					elementAutoPlayHoverPause = false;
				} else {
					elementAutoPlayTime = Number(elementAutoPlay);
					elementAutoPlay = true;
				}
				if (!elementAnimateIn) { elementAnimateIn = false; }
				if (!elementAnimateOut) { elementAnimateOut = false; }
				if (elementNav == 'false') { elementNav = false; } else { elementNav = true; }
				if (elementPagi == 'false') { elementPagi = false; } else { elementPagi = true; }
				if (elementRewind == 'true') { elementRewind = true; } else { elementRewind = false; }
				if (elementMerge == 'true') { elementMerge = true; } else { elementMerge = false; }
				if (elementCenter == 'true') { elementCenter = true; } else { elementCenter = false; }
				if (elementLazy == 'true') { elementLazy = true; } else { elementLazy = false; }
				if (elementVideo == 'true') { elementVideo = true; } else { elementVideo = false; }

				element.owlCarousel({
					margin: Number(elementMargin),
					loop: elementLoop,
					stagePadding: Number(elementStage),
					merge: elementMerge,
					startPosition: Number(elementStart),
					rewind: elementRewind,
					slideBy: elementSlideBy,
					center: elementCenter,
					lazyLoad: elementLazy,
					nav: elementNav,
					navText: ['<i class="fal fa-chevron-left" aria-hidden="true"><span class="sr-only">Previous</span></i>', '<i class="fal fa-chevron-right" aria-hidden="true"><span class="sr-only">Next</span></i>'],
					autoplay: elementAutoPlay,
					autoplayTimeout: elementAutoPlayTime,
					autoplayHoverPause: elementAutoPlayHoverPause,
					dots: elementPagi,
					smartSpeed: Number(elementSpeed),
					fluidSpeed: Number(elementSpeed),
					video: elementVideo,
					animateIn: elementAnimateIn,
					animateOut: elementAnimateOut,
					rtl: elementRTL,
					responsive: {
						0: { items: Number(elementItemsXs) },
						576: { items: Number(elementItemsSm) },
						768: { items: Number(elementItemsMd) },
						992: { items: Number(elementItemsLg) },
						1200: { items: Number(elementItemsXl) },
						1400: { items: Number(elementItemsXXl) },
						1600: { items: Number(elementItemsXXXl) }
					}
				});
			});
		},

		alertMessage: function (message) {

			var alertEl = $('<div class="position-fixed right-0 bottom-0 zindex-100 wishlist-alert"><div class="alert alert-dismissible alert-success">' +
				'<button type="button" class="close" data-dismiss="alert" aria-label="Close">' +
				'&times;</button>' + message + '</div></div>').hide().fadeIn(500);
			$('body').append(alertEl);
			window.setTimeout(function () {
				$(".wishlist-alert").fadeOut(500, function () {
					$(this).remove();
				});
			}, 2000);
		}

	};

	ROLL20.isMobile = {
		Android: function () {
			return navigator.userAgent.match(/Android/i);
		},
		BlackBerry: function () {
			return navigator.userAgent.match(/BlackBerry/i);
		},
		iOS: function () {
			return navigator.userAgent.match(/iPhone|iPad|iPod/i);
		},
		Opera: function () {
			return navigator.userAgent.match(/Opera Mini/i);
		},
		Windows: function () {
			return navigator.userAgent.match(/IEMobile/i);
		},
		any: function () {
			return (ROLL20.isMobile.Android() || ROLL20.isMobile.BlackBerry() || ROLL20.isMobile.iOS() || ROLL20.isMobile.Opera() || ROLL20.isMobile.Windows());
		}
	};

	ROLL20.documentOnResize = {

		init: function () {


		}

	};

	ROLL20.documentOnReady = {

		init: function () {
			ROLL20.marketplace.init();
			ROLL20.widget.init();
		}

	};

	ROLL20.documentOnLoad = {

		init: function () {

		}

	};

	var $window = $(window);

	$(document).ready(ROLL20.documentOnReady.init);
	$window.on('load', ROLL20.documentOnLoad.init);
	$window.on('resize', ROLL20.documentOnResize.init);

	$(document).ready(function () {
		$("#marketplace-filter-panel input[type=checkbox]").on("change", function () {
			let parent = $(this).data("parent");
			let filter = $(this).data("filter");
			if ($(this).prop("checked")) {
				ROLL20.marketplace.searchInput.addItem(`::category::${parent}:${filter}`);
			} else {
				ROLL20.marketplace.searchInput.removeItem(`::category::${parent}:${filter}`, false)
			}
		});

		$("#marketplace-filter-panel__header-clear, #marketplace-filter-panel__active-filters-clear").on("click", function () {
			ROLL20.marketplace.searchInput.clear();
			$("#marketplace-filter-panel input[type=checkbox]").prop("checked", false);
			$("#marketplace-filter-panel").find(".collapse .collapse").collapse("hide");
		});

		$(".pagination li").on("click", function () {
			if (!$(this).hasClass("disabled")) {
				let terms = ROLL20.marketplace.searchTerms();
				terms.page = $(this).data("page");
				ROLL20.marketplace.addHistory(terms);
				ROLL20.marketplace.refreshResults(terms, true);
			}
		});

		$("#input_sort").on("change", function () {
			let terms = ROLL20.marketplace.searchTerms();
			if (terms.page != 1) terms.page = ROLL20.marketplace.currentPage;
			ROLL20.marketplace.addHistory(terms);
			ROLL20.marketplace.refreshResults(terms);
		});

		$("#marketplace-search form").on("submit", function (e) {
			e.preventDefault();
			if (ROLL20.marketplace.homePage) {
				window.location = "/browse/" + ROLL20.marketplace.searchUrl;
			}
		});

		$("#marketplace-filter-panel__search-btn").on("click", function () {
			$('body').toggleClass("marketplace-filter-panel-open overflow-hidden overlay-visible");
			if (ROLL20.marketplace.homePage) {
				window.location = "/browse/" + ROLL20.marketplace.searchUrl;
			}
		});

		$(".wishlist-dropdown-menu").on("click", ".wishlist-dropdown-menu .wishlist-dropdown-item.wl:not(.disabled)", function () {
			let listid = $(this).data("listid");
			let itemid = $(this).parent().parent().data("itemid");
			ROLL20.marketplace.addItemToWishlist(listid, itemid);
		});

		$("#createNewWishlistModal form").on("submit", function (e) {
			e.preventDefault();
			let values = $("#createNewWishlistModal form input").serialize();
			let itemid = $("#createNewWishlistModal input[name=itemid]").val();
			$.ajax({
				url: `/wishlist/create`,
				data: values,
				type: 'post',
				dataType: 'json'
			}).done(function (data) {
				let newList = _.last(data);
				$("#createNewWishlistModal").modal('hide');
				$("#createNewWishlistModal form")[0].reset();
				let thislist = `<div class="dropdown-item wishlist-dropdown-item wl" data-listid="${newList.id}">`;
				thislist += `<a href="javascript:void(0)" tabindex="-1" class="disabled text-muted" aria-disabled="true">`;
				thislist += `<div class="d-flex justify-content-between flex-column flex-xxl-row"><div class="mr-2">`;
				thislist += `<div class="wishlist-dropdown-item__title d-block">${newList.name}</div>`;
				thislist += `<div class="wishlist-dropdown-item__privacy small">${newList.public ? translations.wishlist_public : translations.wishlist_private}</div></div>`;
				thislist += `<div><div class="wishlist-dropdown-item__items text-xxl-right small">${translations.wishlist_items.replace("{{0}}", newList.items.length)}</div>`;
				thislist += `</div></div><div class="wishlist-dropdown-item__in-wishlist small text-tertiary">${translations.wishlist_in_list}</div></a></div>`;
				$(".wishlist-dropdown-item.empty-message").before(thislist);
				$(`.wishlist-dropdown-menu[data-itemid=${itemid}] .wishlist-dropdown-item.wl[data-listid=${newList.id}]`).addClass("disabled").attr("aria-disabled", "true");
				ROLL20.widget.alertMessage(translations.wishlist_alert_add_item);
			});
		});
	});

	$window.on("popstate", function () {
		ROLL20.marketplace.ignoreChange = true;
		$("#marketplace-filter-panel").find("[data-toggle] [aria-expanded]").attr("aria-expanded", "false");
		$("#marketplace-filter-panel").find("[data-toggle] [data-toggle]").addClass("collapsed");
		$("#marketplace-filter-panel").find("[data-toggle] .show").removeClass("show");
		$("#marketplace-filter-panel [type=checkbox]").prop("checked", false);
		ROLL20.marketplace.searchInput.clear();
		ROLL20.marketplace.addSearchTerms(history.state);
		ROLL20.marketplace.refreshResults(history.state);
		ROLL20.marketplace.ignoreChange = false;
	});

})(jQuery);