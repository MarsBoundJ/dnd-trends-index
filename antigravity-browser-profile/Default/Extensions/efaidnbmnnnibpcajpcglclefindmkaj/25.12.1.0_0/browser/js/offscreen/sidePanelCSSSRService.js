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
import{extractWebpageHTML as e}from"../../../resources/addWebpage/extractWebpageHTML.js";import{largeBlobStorage as t}from"../../../common/large-blob-storage.js";import{dcLocalStorage as n}from"./offscrenUtil.js";export const saveGenAISidePanelRenderedHTML=async o=>{if(document.querySelector("iframe#sidepanel"))return;const r=await n.getItem("sidepanelUrl"),a=await n.getItem("appLocale")||chrome.i18n.getMessage("@@ui_locale"),s=new URL(r),i=s.origin;s.searchParams.append("la",!1),s.searchParams.append("ca",chrome.runtime.id),s.searchParams.append("locale",a),s.searchParams.append("utl",!0);const m=(e=>{const t=document.createElement("iframe");return t.setAttribute("id","sidepanel"),t.width="373px",t.height="914px",t.border="none",t.onerror=e=>{console.error("Error in loading sidepanel iframe",e)},t.src=e,t})(s.href);document.body.appendChild(m);const l=(()=>{let e;return{promise:new Promise((t,n)=>{const o=setTimeout(()=>{n(new Error("Hosted sidepanel ready event timeout"))},2e4);e=()=>{clearTimeout(o),t()}}),resolver:e}})(),c=e=>{m&&l.promise.then(()=>m.contentWindow.postMessage(e,i)).catch(e=>{console.error("Error in sending message to sidepanel:",e.toString())})};window.addEventListener("message",async n=>{if(n.origin===i)switch(n.data.main_op){case"saveHtmlContent":await async function(n){const o=(new DOMParser).parseFromString(n.htmlContent,"text/html");console.log("document from CDN",o.documentElement);const r=await e(o);console.log(r),!0===n?.isAnonUser&&r?.html?(await t.setItem("anonGenAISSRHtml",r.html),c({type:"sidepanelHtmlRenderSaved"})):m.remove()}(n.data);break;case"cdnReady":l.resolver();break;case"exHostedDeploymentVersionUpdated":m.remove()}}),c({type:"sidepanelHtmlContent",htmlContent:"<html><body><h1>Hello</h1></body></html>",disqualified:!1,pageLanguage:"en",url:"https://www.hello.com",touchpoint:"offscreen"})};