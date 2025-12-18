/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/
async function e(e){try{const o=await fetch(e);if(!o.ok)throw new Error(`Failed to load ${e}`);const r=await o.text(),n=(new DOMParser).parseFromString(r,"text/html"),c=n.head.querySelectorAll("link, style");for(const e of c)document.head.appendChild(e.cloneNode(!0));const a=n.querySelector("title");a&&(document.title=a.textContent);const s=n.head.querySelectorAll('script:not([type="module"])');for(const e of s)await t(e,"head");const l=n.head.querySelectorAll('script[type="module"]');for(const e of l)await t(e,"head");n.body.className&&(document.body.className=n.body.className),n.body.style.cssText&&(document.body.style.cssText=n.body.style.cssText);const d=document.getElementById("options-app-container");if(!d)throw new Error("Container element not found");const i=n.body.cloneNode(!0);i.querySelectorAll("script").forEach(e=>e.remove()),d.innerHTML=i.innerHTML;const m=n.body.querySelectorAll('script:not([type="module"])');for(const e of m)await t(e,"body");const y=n.body.querySelectorAll('script[type="module"]');for(const e of y)await t(e,"body")}catch(t){console.error(`Error loading HTML from ${e}:`,t)}}function t(e,t="body"){return new Promise((o,r)=>{const n=document.createElement("script"),c="head"===t?document.head:document.body;if(Array.from(e.attributes).forEach(e=>{n.setAttribute(e.name,e.value)}),!e.src&&e.textContent)return n.textContent=e.textContent,c.appendChild(n),void o();e.src?(n.onload=o,n.onerror=()=>r(new Error(`Failed to load script: ${e.src}`)),c.appendChild(n)):o()})}!async function(){try{await chrome.runtime.sendMessage({main_op:"getFloodgateFlag",flag:"dc-cv-new-preferences",cachePurge:"NO_CALL"})?await e(chrome.runtime.getURL("browser/js/preference-page.html")):await e(chrome.runtime.getURL("browser/js/legacy-options.html"))}catch(t){console.error("Error in options router:",t),await e(chrome.runtime.getURL("browser/js/legacy-options.html"))}}();